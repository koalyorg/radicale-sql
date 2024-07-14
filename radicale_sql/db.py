#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import uuid
import datetime
from typing import Tuple
import sqlalchemy as sa


def create_meta() -> sa.MetaData:
    meta = sa.MetaData()

    sa.Table(
        'collection',
        meta,
        sa.Column(
            'id',
            sa.Uuid(),
            default=uuid.uuid4,
            primary_key=True,
        ),
        sa.Column(
            'parent_id',
            sa.Uuid(),
            sa.ForeignKey("collection.id"),
            index=True,
            nullable=True,
        ),
        sa.Column(
            'modified',
            sa.DateTime(),
            default=datetime.datetime.now,
            onupdate=datetime.datetime.now,
            nullable=False,
        ),
        sa.Column(
            'name',
            sa.String(128),
            index=True,
            nullable=True,
        ),
        sa.UniqueConstraint('parent_id', 'name'),
    )

    sa.Table(
        'collection_metadata',
        meta,
        sa.Column(
            'collection_id',
            sa.Uuid(),
            sa.ForeignKey('collection.id', ondelete='CASCADE'),
            primary_key=True,
        ),
        sa.Column(
            'key',
            sa.String(length=128),
            primary_key=True,
        ),
        sa.Column(
            'value',
            sa.Text(),
        ),
        sa.UniqueConstraint('collection_id', 'key'),
    )

    sa.Table(
        'collection_state',
        meta,
        sa.Column(
            'collection_id',
            sa.Uuid(),
            sa.ForeignKey('collection.id', ondelete='CASCADE'),
            index=True,
            nullable=False,
        ),
        sa.Column(
            'name',
            sa.String(length=128),  # could be only 64 long
            index=True,
            nullable=False,
        ),
        sa.Column(
            'state',
            sa.LargeBinary(),
            nullable=False,
        ),
    )

    sa.Table(
        'item',
        meta,
        sa.Column(
            'id',
            sa.Uuid(),
            default=uuid.uuid4,
            primary_key=True,
        ),
        sa.Column(
            'collection_id',
            sa.Uuid(),
            sa.ForeignKey("collection.id", ondelete='CASCADE'),
            index=True,
            nullable=False,
        ),
        sa.Column(
            'modified',
            sa.DateTime(),
            default=datetime.datetime.now,
            onupdate=datetime.datetime.now,
            nullable=False,
        ),
        sa.Column(
            'name',
            sa.String(128),
            index=True,
            nullable=True,
        ),
        sa.Column(
            'data',
            sa.LargeBinary(),
        ),
        sa.UniqueConstraint('collection_id', 'name'),
    )

    sa.Table(
        'item_history',
        meta,
        sa.Column(
            'id',
            sa.Uuid(),
            default=uuid.uuid4,
            primary_key=True,
        ),
        sa.Column(
            'collection_id',
            sa.Uuid(),
            sa.ForeignKey("collection.id", ondelete='CASCADE'),
            index=True,
            nullable=False,
        ),
        sa.Column(
            'modified',
            sa.DateTime(),
            default=datetime.datetime.now,
            onupdate=datetime.datetime.now,
            nullable=False,
        ),
        sa.Column(
            'name',
            sa.String(128),
            index=True,
            nullable=True,
        ),
        sa.Column(
            'etag',
            sa.String(1024),
            nullable=False,
        ),
        sa.Column(
            'history_etag',
            sa.String(1024),
            nullable=True,
        )
    )

    return meta


def create(url: str, meta: sa.MetaData) -> Tuple[sa.engine.Engine, sa.engine.Row]:
    engine = sa.create_engine(url)
    meta.create_all(engine)

    collection = meta.tables['collection']
    with engine.begin() as connection:
        select_root_collection = sa.select(
            collection.c
        ).select_from(
            collection
        ).where(
            collection.c.parent_id == None,
        )
        root_collection = connection.execute(select_root_collection).one_or_none()
        if root_collection is None:
            insert_root_collection = sa.insert(
                collection,
            ).values(parent_id=None).returning(collection.c)
            root_collection = connection.execute(insert_root_collection).one()

    return engine, root_collection


if __name__ == '__main__':
    meta = create_meta()
    engine = create('sqlite:///test.db', meta)
