#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import binascii
import datetime
import uuid
import string
import itertools
import json
from hashlib import sha256
from typing import Optional, Union, Tuple, Iterable, Iterator, Mapping

import radicale
import radicale.config
import radicale.types
from radicale.storage import BaseStorage, BaseCollection
from radicale.log import logger
from radicale import item as radicale_item
import sqlalchemy as sa

from . import db

PLUGIN_CONFIG_SCHEMA = {
    'storage': {
        'db_url': {
            'value': '',
            'type': str,
        },
    },
}

class Collection(BaseCollection):

    def __init__(self, storage: "Storage", id: uuid.UUID, path: str):
        self._storage = storage
        self._id = id
        self._path = path

    def __repr__(self) -> str:
        return f'Collection(id={self._id}, path={self._path})'

    @property
    def path(self) -> str:
        return self._path

    def _row_to_item(self, row):
        return radicale_item.Item(
            collection=self,
            href=row.name,
            last_modified=row.modified,
            text=row.data.decode(),
        )

    def get_multi(self, hrefs: Iterable[str]) -> Iterable[Tuple[str, Optional["radicale_item.Item"]]]:
        item_table = self._storage._meta.tables['item']
        hrefs_ = list(hrefs)
        #hrefs_ = [(x,) for x in hrefs]
        if not hrefs_:
            return []
        select_stmt = sa.select(
            item_table.c,
        ).select_from(
            item_table,
        ).where(
            sa.and_(
                item_table.c.collection_id == self._id,
                item_table.c.name.in_(hrefs_)
            ),
        )
        l = []
        with self._storage._engine.begin() as connection:
            for row in connection.execute(select_stmt):
                l += [(row.name, self._row_to_item(row))]
        hrefs_set = set(hrefs_)
        hrefs_set_have = set([x[0] for x in l])
        l += [(x, None) for x in (hrefs_set - hrefs_set_have)]
        return l

    def get_all(self) -> Iterator["radicale_item.Item"]:
        item_table = self._storage._meta.tables['item']
        select_stmt = sa.select(
            item_table.c,
        ).select_from(
            item_table,
        ).where(
            item_table.c.collection_id == self._id,
        )
        with self._storage._engine.begin() as connection:
            for row in connection.execute(select_stmt):
                yield self._row_to_item(row)

    def upload(self, href: str, item: "radicale_item.Item") -> "radicale_item.Item":
        item_table = self._storage._meta.tables['item']

        item_serialized = item.serialize().encode()
        select_stmt = sa.select(
            item_table.c,
        ).select_from(
            item_table,
        ).where(
            sa.and_(
                item_table.c.collection_id == self._id,
                item_table.c.name == href,
            ),
        )
        insert_stmt = sa.insert(
            item_table,
        ).values(
            collection_id=self._id,
            name=href,
            data=item_serialized,
        )
        update_stmt = sa.update(
            item_table,
        ).values(
            data=item_serialized,
        ).where(
            sa.and_(
                item_table.c.collection_id == self._id,
                item_table.c.name == href,
            ),
        )
        with self._storage._engine.begin() as connection:
            if connection.execute(select_stmt).one_or_none() is None:
                connection.execute(insert_stmt)
            else:
                connection.execute(update_stmt)
        res = list(self.get_multi([href]))[0][1]
        assert res is not None
        return res

    def delete(self, href: Optional[str] = None) -> None:
        collection_table = self._storage._meta.tables['collection']
        item_table = self._storage._meta.tables['item']
        if href is None:
            delete_stmt = sa.delete(
                collection_table,
            ).where(
                collection_table.c.id == self._id,
            )
        else:
            delete_stmt = sa.delete(
                item_table,
            ).where(
                sa.and_(
                    item_table.c.collection_id == self._id,
                    item_table.c.name == href,
                ),
            )
        with self._storage._engine.begin() as connection:
            connection.execute(delete_stmt)

    def get_meta(self, key: Optional[str] = None) -> Union[Mapping[str, str], Optional[str]]:
        collection_metadata = self._storage._meta.tables['collection_metadata']
        select_meta = sa.select(
            collection_metadata.c.key,
            collection_metadata.c.value,
        ).select_from(
            collection_metadata,
        ).where(
            collection_metadata.c.collection_id == self._id,
        )
        if key is not None:
            select_meta = select_meta.where(
                collection_metadata.c.key == key,
            )
        metadata = {}
        with self._storage._engine.begin() as connection:
            for row in connection.execute(select_meta):
                metadata[row.key] = row.value
        if key is not None:
            return metadata.get(key)
        return metadata

    def set_meta(self, props: Mapping[str, str]) -> None:
        collection_metadata = self._storage._meta.tables['collection_metadata']
        delete_stmt = sa.delete(
            collection_metadata,
        ).where(
            collection_metadata.c.collection_id == self._id,
        )
        insert_stmt = sa.insert(
            collection_metadata,
        ).values([dict(collection_id=self._id, key=k, value=v) for k, v in props.items()])
        with self._storage._engine.begin() as connection:
            connection.execute(delete_stmt)
            connection.execute(insert_stmt)

    @property
    def last_modified(self) -> str:
        collection = self._storage._meta.tables['collection']
        select_stmt = sa.select(
            collection.c.modified,
        ).select_from(
            collection,
        ).where(
            collection.c.id == self._id,
        )
        with self._storage._engine.begin() as connection:
            c = connection.execute(select_stmt).one()
        return c.modified.strftime('%a, %d %b %Y %H:%M:%S GMT')

    def _update_history_etag(self, href: str, item: Optional["radicale_item.Item"]) -> str:
        item_history_table = self._storage._meta.tables['item_history']
        select_etag_stmt = sa.select(
            item_history_table.c,
        ).select_from(
            item_history_table,
        ).where(
            sa.and_(
                item_history_table.c.collection_id == self._id,
                item_history_table.c.name == href,
            ),
        )
        exists: bool
        with self._storage._engine.begin() as connection:
            item_history = connection.execute(select_etag_stmt).one_or_none()
            if item_history is not None:
                exists = True
                cache_etag = item_history.etag,
                history_etag = item_history.history_etag
            else:
                exists = False
                cache_etag = ''
                history_etag = binascii.hexlify(os.urandom(16)).decode('ascii')
            etag = item.etag if item else ''
            if etag != cache_etag:
                if exists:
                    upsert = sa.update(
                        item_history_table,
                    ).values(
                        etag=etag,
                        history_etag=history_etag,
                    ).where(
                        sa.and_(
                            item_history_table.c.collection_id == self._id,
                            item_history_table.c.name == href,
                        ),
                    )
                else:
                    upsert = sa.insert(
                        item_history_table,
                    ).values(
                        collection_id=self._id,
                        name=href,
                        etag=etag,
                        history_etag=history_etag,
                    )
                connection.execute(upsert)
        return history_etag

    def _get_deleted_history_refs(self):
        item_table = self._storage._meta.tables['item']
        item_history_table = self._storage._meta.tables['item_history']
        select_stmt = sa.select(
            item_history_table.c.name,
        ).select_from(
            item_history_table.join(
                item_table,
                sa.and_(
                    item_history_table.c.collection_id == item_table.c.collection_id,
                    item_history_table.c.name == item_table.c.name,
                ),
                isouter=True,
            ),
        ).where(
            item_table.c.id == None,
        )
        with self._storage._engine.begin() as connection:
            for row in connection.execute(select_stmt):
                yield row.href

    def _delete_history_refs(self):
        item_history_table = self._storage._meta.tables['item_history']
        delete_stmt = sa.delete(
            item_history_table,
        ).where(
            sa.and_(
                item_history_table.c.href.in_(list(self._get_deleted_history_refs())),
                item_history_table.c.collection_id == self._id,
                item_history_table.c.modified < (datetime.datetime.now() - datetime.timedelta(seconds=self._storage.configuration.get('storage', 'max_sync_token_age')))
            ),
        )
        with self._storage._engine.begin() as connection:
            connection.execute(delete_stmt)

    def sync(self, old_token: str = '') -> Tuple[str, Iterable[str]]:
        _prefix = 'http://radicale.org/ns/sync/'
        collection_state_table = self._storage._meta.tables['collection_state']
        def check_token_name(token_name: str) -> bool:
            print(token_name)
            print(len(token_name))
            if len(token_name) != 64:
                return False
            for c in token_name:
                if c not in string.hexdigits.lower():
                    return False
            return True

        old_token_name = ''
        if old_token:
            if not old_token.startswith(_prefix):
                raise ValueError(f'Malformed token: {old_token}')
            old_token_name = old_token[len(_prefix):]
            if not check_token_name(old_token_name):
                raise ValueError(f'Malformed token: {old_token}')
        state = {}
        token_name_hash = sha256()

        # compute new state
        for href, item in itertools.chain(
            ((item.href, item) for item in self.get_all()),
            ((href, None) for href in self._get_deleted_history_refs())
        ):
            assert isinstance(href, str)
            history_etag = self._update_history_etag(href, item)
            state[href] = history_etag
            token_name_hash.update((href + '/' + history_etag).encode())
        token_name = token_name_hash.hexdigest()
        token = _prefix + token_name

        # if new state hasn't changed: dont send any updates
        if token_name == old_token_name:
            return token, ()

        # load old state
        old_state = {}
        with self._storage._engine.begin() as connection:
            if old_token_name:
                select_stmt = sa.select(
                    collection_state_table.c,
                ).select_from(
                    collection_state_table,
                ).where(
                    sa.and_(
                        collection_state_table.c.collection_id == self._id,
                        collection_state_table.c.name == old_token_name,
                    ),
                )
                state_row = connection.execute(select_stmt).one()
                state = json.loads(state_row.state.decode())
        
            # store new state
            ## should never be a duplicate
            insert_stmt = sa.insert(
                collection_state_table,
            ).values(
                collection_id=self._id,
                name=token_name,
                state=json.dumps(state).encode(),
            )
            connection.execute(insert_stmt)

        changes = []
        for href, history_etag in state.items():
            if history_etag != old_state.get(href):
                changes += [href]
        for href, history_etag in old_state.items():
            if href not in state:
                changes += [href]

        return token, changes

class Storage(BaseStorage):

    def __init__(self, configuration: "radicale.config.Configuration"):
        super().__init__(configuration)
        self._meta = db.create_meta()
        self._engine, self._root_collection = db.create(self.configuration.get('storage', 'url'), self._meta)

    def _split_path(self, path: str):
        path_parts = path.split('/')
        if path_parts[0] == '':
            path_parts = path_parts[1:]
        if path_parts[-1] == '':
            path_parts = path_parts[:-1]
        return path_parts

    def discover(self, path: str, depth: str = "0") -> Iterable["radicale.types.CollectionOrItem"]:
        logger.info("path = %s, depth = %s", path, depth)
        if path == '/':
            return [Collection(self, self._root_collection.id, '')]
        path_parts = self._split_path(path)

        collection_table = self._meta.tables['collection']
        item_table = self._meta.tables['item']

        select_collection_or_item = sa.select(
            collection_table.c.id,
            collection_table.c.parent_id.label('parent_id'),
            collection_table.c.modified,
            collection_table.c.name,
            sa.literal(None, sa.LargeBinary()).label('data'),
            sa.literal('collection', sa.String(16)).label('type_'),
        ).select_from(
            collection_table
        ).union_all(sa.select(
            item_table.c.id,
            item_table.c.collection_id.label('parent_id'),
            item_table.c.modified,
            item_table.c.name,
            item_table.c.data,
            sa.literal('item', sa.String(16)).label('type_'),
        ).select_from(
            item_table
        ))
        i = 0
        select_from = select_collection_or_item.alias('data')
        aliases = [select_from]
        for path in path_parts[::-1]:
            aliases += [select_collection_or_item.alias(f't{i}')]
            i += 1
            select_from = select_from.join(
                aliases[-1],
                sa.and_(
                    aliases[-2].c.parent_id == aliases[-1].c.id,
                    aliases[-2].c.name == path,
                ),
            )
        select_stmt = sa.select(
            aliases[0].c,
        ).select_from(
            select_from
        ).where(
            aliases[-1].c.parent_id == None,
        )
        select_sub_stmt = None
        if depth != "0":
            aliased = select_collection_or_item.alias('data_list')
            select_sub_stmt = sa.select(
                aliased.c,
            ).select_from(
                aliased.join(
                    select_from,
                    aliased.c.parent_id == aliases[0].c.id,
                ),
            ).where(
                aliases[-1].c.parent_id == None,
            )

        l = []
        with self._engine.begin() as connection:
            self_collection = connection.execute(select_stmt).one_or_none()
            if self_collection is None:
                return []
            self_collection = Collection(self, self_collection.id, '/'.join(path_parts))
            l += [self_collection]
            if select_sub_stmt is not None:
                for row in connection.execute(select_sub_stmt):
                    path = '/'.join(path_parts)
                    path += '/'
                    path += row.name
                    if row.type_ == 'collection':
                        l += [Collection(self, row.id, path)]
                    elif row.type_ == 'item':
                        assert self_collection is not None
                        l += [self_collection._row_to_item(row)]
        return l


    def move(self, item: "radicale_item.Item", to_collection: "BaseCollection", to_href: str) -> None:
        assert isinstance(item.collection, Collection)
        assert isinstance(to_collection, Collection)
        src_collection_id = item.collection._id
        dst_collection_id = to_collection._id
        item_table = self._meta.tables['item']

        delete_stmt = sa.delete(
            item_table,
        ).where(
            sa.and_(
                item_table.c.collection_id == dst_collection_id,
                item_table.c.name == to_href,
            )
        )
        update_stmt = sa.update(
            item_table,
        ).values(
            collection_id=dst_collection_id,
            name=to_href,
        ).where(
            sa.and_(
                item_table.c.collection_id == src_collection_id,
                item_table.c.name == item.href,
            )
        )
        with self._engine.begin() as connection:
            connection.execute(delete_stmt)
            connection.execute(update_stmt)

    def create_collection(
        self,
        href: str,
        items: Optional[Iterable["radicale_item.Item"]]=None,
        props: Optional[Mapping[str, str]]=None,
    ) -> "BaseCollection":
        print('creating collection')
        print(f'href={href}, items={items}, props={props}')
        path = self._split_path(href)
        parent_id = self._root_collection.id
        collection_table = self._meta.tables['collection']
        collection_metadata_table = self._meta.tables['collection_metadata']
        item_table = self._meta.tables['item']

        with self._engine.begin() as connection:
            for p in path:
                select_stmt = sa.select(
                    collection_table.c,
                ).select_from(
                    collection_table,
                ).where(
                    sa.and_(
                        collection_table.c.parent_id == parent_id,
                        collection_table.c.name == p,
                    ),
                )
                c = connection.execute(select_stmt).one_or_none()
                if c is None:
                    insert_stmt = sa.insert(
                        collection_table,
                    ).values(
                        parent_id=parent_id,
                        name=p,
                    ).returning(
                        collection_table.c,
                    )
                    c = connection.execute(insert_stmt).one()
                parent_id = c.id
            if items is not None or props is not None:
                # drop all subcollections and items
                delete_collections_stmt = sa.delete(
                    collection_table,
                ).where(
                    collection_table.c.parent_id == parent_id,
                )
                delete_meta_stmt = sa.delete(
                    collection_metadata_table,
                ).where(
                    collection_metadata_table.c.collection_id == parent_id,
                )
                delete_items_stmt = sa.delete(
                    item_table,
                ).where(
                    item_table.c.collection_id == parent_id,
                )
                connection.execute(delete_collections_stmt)
                connection.execute(delete_meta_stmt)
                connection.execute(delete_items_stmt)
            if props is not None:
                insert_stmt = sa.insert(
                    collection_metadata_table,
                ).values([dict(collection_id=parent_id, key=k, value=v) for k, v in props.items()])
                connection.execute(insert_stmt)
            if props is not None and 'key' in props and items is not None:
                print(items)
                # TODO insert items
        c = Collection(self, parent_id, '/'.join(path))
        print(c)
        return c



    @radicale.types.contextmanager
    def acquire_lock(self, mode: str, user: str = "") -> Iterator[None]:
        # locking happens on a db level
        yield

    def verify(self) -> bool:
        return True
