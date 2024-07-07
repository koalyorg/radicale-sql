#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import uuid
import asyncio
import datetime
import caldav
import caldav.lib

_BASE_URL = 'http://localhost:5232/'
_PASSWORD = 'test'
_USER = 'test'


def test_caldav():
    client0 = None
    try:

        # create calendars
        client0 = caldav.DAVClient(url=_BASE_URL, username='test', password='test')
        principal0 = client0.principal()
        calendar0 = principal0.make_calendar(name=f'test-calendar-{uuid.uuid4()}')
        print(f'calendar url = {calendar0.url}')

        print(calendar0.events())
        calendar0_events = set([x.url for x in calendar0.events()])
        assert calendar0_events == set()

        # create event and store it
        calendar0_events |= {calendar0.save_event(
            dtstart=datetime.datetime.now(),
            dtend=datetime.datetime.now() + datetime.timedelta(hours=1),
            summary='event0',
        ).url}

        # obtain sync token for first event
        calendar0_updates = calendar0.objects_by_sync_token()
        calendar0_token = calendar0_updates.sync_token
        assert set([x.url for x in calendar0_updates]) == calendar0_events, (
        set(calendar0_updates), set(calendar0_events))

        # get changes with sync token (should give no difference)
        # do this for both calendars
        calendar0_updates = calendar0.objects_by_sync_token(calendar0_token)
        assert set(calendar0_updates) == set()

        # add another event to the calendar
        calendar0_events |= {calendar0.save_event(
            dtstart=datetime.datetime.now(),
            dtend=datetime.datetime.now() + datetime.timedelta(hours=1),
            summary='event0',
        ).url}

        calendar0_updates = calendar0.objects_by_sync_token(calendar0_token)
        assert len(set(calendar0_updates)) == 1
        calendar0_token = calendar0_updates.sync_token

        # check that sync token returns 0 updates
        calendar0_updates = calendar0.objects_by_sync_token(calendar0_token)
        assert set(calendar0_updates) == set()

        # update event
        calendar0_any_event = calendar0.event_by_url(list(calendar0_events)[0])
        calendar0_any_event.load()
        calendar0_any_event.vobject_instance.vevent_list[0].summary.value = 'event0-edit0'
        calendar0_any_event.save()

        # check that we get the edited event
        calendar0_updates = calendar0.objects_by_sync_token(calendar0_token)
        assert len(set(calendar0_updates)) == 1
        calendar0_token = calendar0_updates.sync_token

        # delete this event
        calendar0_any_event = calendar0.event_by_url(list(calendar0_events)[0])
        calendar0_any_event.delete()

        calendar0_updates = calendar0.objects_by_sync_token(calendar0_token)
        assert len(list(calendar0_updates)) == 1
        try:
            list(calendar0_updates)[0].load()
        except caldav.lib.error.NotFoundError:
            pass
        else:
            assert False

        # changes = calendar.objects_by_sync_token(load_objects=True)
        # token = changes.sync_token
    except:
        print('failed')
        raise
    else:
        print('success')
    finally:
        if client0 is not None:
            client0.close()


if __name__ == '__main__':
    test_caldav()
