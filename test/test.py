import os
import unittest
import subprocess
import time
import requests
import vobject
from requests.auth import HTTPBasicAuth
from passlib.apache import HtpasswdFile
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurations
config_path = os.path.abspath('./radicale_config')
database_path = os.path.abspath('./test-data.db')
htpasswd_path = os.path.abspath('./.htpasswd')
radicale_port = 5232
radicale_host = '127.0.0.1'
radicale_url = f'http://{radicale_host}:{radicale_port}/'

CREATE_CALENDAR_XML = '''<?xml version="1.0" encoding="UTF-8" ?>
<create xmlns="DAV:" xmlns:CAL="urn:ietf:params:xml:ns:caldav">
  <set>
    <prop>
      <resourcetype>
        <collection />
        <CAL:calendar />
      </resourcetype>
    </prop>
  </set>
</create>'''

ics_contents = {
    'user1': """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:uid1@example.com
DTSTAMP:20200714T170000Z
ORGANIZER;CN=User One:MAILTO:user1@example.com
DTSTART:20200714T170000Z
DTEND:20200714T180000Z
SUMMARY:User One Event
END:VEVENT
END:VCALENDAR""",
    'user2': """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:uid2@example.com
DTSTAMP:20200714T170000Z
ORGANIZER;CN=User Two:MAILTO:user2@example.com
DTSTART:20200714T170000Z
DTEND:20200714T180000Z
SUMMARY:User Two Event
END:VEVENT
END:VCALENDAR""",
    'user3': """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:uid3@example.com
DTSTAMP:20200714T170000Z
ORGANIZER;CN=User Three:MAILTO:user3@example.com
DTSTART:20200714T170000Z
DTEND:20200714T180000Z
SUMMARY:User Three Event
END:VEVENT
END:VCALENDAR"""
}


class TestRadicaleServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            if os.path.exists(database_path):
                os.remove(database_path)
            if os.path.exists(htpasswd_path):
                os.remove(htpasswd_path)
            if os.path.exists(config_path):
                os.remove(config_path)

            # Create .htpasswd file with user credentials
            ht = HtpasswdFile(htpasswd_path, new=True)
            ht.set_password('user1', 'password')
            ht.set_password('user2', 'password')
            ht.set_password('user3', 'password')
            ht.save()

            # Update Radicale config to use .htpasswd file
            with open(config_path, 'w') as config_file:
                config_file.write(f"""
[auth]
type = htpasswd
htpasswd_filename = {htpasswd_path}
htpasswd_encryption = md5
[server]
hosts = {radicale_host}:{radicale_port}
[storage]
type=radicale_sql
url=sqlite:///{database_path}
""")

            cls.process = subprocess.Popen(
                ['radicale', '--config', config_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Wait for the server to start
            for _ in range(10):
                if cls.process.poll() is not None:
                    raise Exception("Radicale server terminated prematurely")
                try:
                    response = requests.get(radicale_url)
                    if response.status_code == 200:
                        logger.info("Radicale server started successfully")
                        break
                except requests.ConnectionError:
                    time.sleep(0.5)
            else:
                raise Exception("Radicale server did not start within the expected time")

        except Exception as e:
            cls.tearDownClass()
            raise e

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'process'):
            cls.process.terminate()
            cls.process.wait()
        if os.path.exists(database_path):
            os.remove(database_path)
        if os.path.exists(htpasswd_path):
            os.remove(htpasswd_path)
        if os.path.exists(config_path):
            os.remove(config_path)

    def test_radicale_is_running(self):
        response = requests.get(radicale_url)
        self.assertEqual(response.status_code, 200)

    def create_collection(self, username, password, collection):
        url = f'{radicale_url}{username}/{collection}/'
        response = requests.request('MKCOL', url, data=CREATE_CALENDAR_XML, auth=HTTPBasicAuth(username, password))
        self.assertIn(response.status_code, [201, 204])

        response = requests.request('PROPFIND', url, auth=HTTPBasicAuth(username, password))
        self.assertEqual(response.status_code, 207)

    def add_ics_file(self, username, password, collection, filename, content, return_code=[201, 204]):
        url = f'{radicale_url}{username}/{collection}/{filename}'
        headers = {'Content-Type': 'text/calendar'}
        response = requests.put(url, data=content, headers=headers, auth=(username, password))
        self.assertIn(response.status_code, return_code)

    def parse_ics(self, content):
        return vobject.readOne(content)

    def test_user_access(self):
        users = ['user1', 'user2', 'user3']
        password = 'password'

        # Create collections and add .ics files for each user
        for user, ics_content in ics_contents.items():
            with self.subTest(user=user):
                self.create_collection(user, password, 'calendar')
                self.add_ics_file(user, password, 'calendar', 'event.ics', ics_content)

        # Verify that each user can only access their own collection and data
        for user, ics_content in ics_contents.items():
            with self.subTest(user=user):
                response = requests.get(f'{radicale_url}{user}/calendar/event.ics', auth=(user, password))
                self.assertEqual(response.status_code, 200)

                parsed_response = self.parse_ics(response.text)
                parsed_ics_content = self.parse_ics(ics_content)

                self.assertEqual(parsed_response.serialize(), parsed_ics_content.serialize())

                for other_user in users:
                    if other_user != user:
                        response = requests.get(f'{radicale_url}{other_user}/calendar/event.ics', auth=(user, password))
                        self.assertEqual(response.status_code, 403)

        for user, ics_content in ics_contents.items():
            with self.subTest(user=user):
                self.delete_collection(user, password, 'calendar')

    def test_invalid_authentication(self):
        response = requests.get(f'{radicale_url}user1/calendar/', auth=('user1', 'wrongpassword'))
        self.assertEqual(response.status_code, 401)

    def delete_collection(self, username, password, collection):
        url = f'{radicale_url}{username}/{collection}/'
        response = requests.request('DELETE', url, auth=HTTPBasicAuth(username, password))
        self.assertIn(response.status_code, [200, 204, 404])

    def test_delete_collection(self):
        username = 'user1'
        password = 'password'
        collection = 'calendar'
        self.create_collection(username, password, collection)
        self.delete_collection(username, password, collection)
        response = requests.get(f'{radicale_url}{username}/{collection}/', auth=(username, password))
        self.assertEqual(response.status_code, 404)

    def update_ics_file(self, username, password, collection, filename, new_content):
        url = f'{radicale_url}{username}/{collection}/{filename}'
        headers = {'Content-Type': 'text/calendar'}
        response = requests.put(url, data=new_content, headers=headers, auth=(username, password))
        self.assertIn(response.status_code, [201, 204])

    def test_update_ics_file(self):
        username = 'user1'
        password = 'password'
        collection = 'calendar'
        filename = 'event.ics'
        new_content = """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:uid1@example.com
DTSTAMP:20200714T170000Z
ORGANIZER;CN=User One:MAILTO:user1@example.com
DTSTART:20200715T170000Z
DTEND:20200715T180000Z
SUMMARY:Updated User One Event
END:VEVENT
END:VCALENDAR"""
        self.create_collection(username, password, collection)
        self.add_ics_file(username, password, collection, filename, ics_contents['user1'])
        self.update_ics_file(username, password, collection, filename, new_content)
        response = requests.get(f'{radicale_url}{username}/{collection}/{filename}', auth=(username, password))
        self.assertEqual(response.status_code, 200)
        self.assertIn("Updated User One Event", response.text)
        self.delete_collection(username, password, collection)

    def test_fetch_nonexistent_ics_file(self):
        self.create_collection('user1', 'password', 'calendar')
        response = requests.get(f'{radicale_url}user1/calendar/nonexistent.ics', auth=('user1', 'password'))
        self.assertEqual(response.status_code, 404)
        self.delete_collection('user1', 'password', 'calendar')

    def test_collection_permissions(self):
        # Assuming 'user1' tries to access 'user2's collection
        self.create_collection('user1', 'password', 'calendar')
        self.create_collection('user2', 'password', 'calendar')
        response = requests.get(f'{radicale_url}user2/calendar/', auth=('user1', 'password'))
        self.assertEqual(response.status_code, 403)
        self.delete_collection('user1', 'password', 'calendar')
        self.delete_collection('user2', 'password', 'calendar')

    def test_large_ics_file_handling_single_event(self):
        username = 'user1'
        password = 'password'
        collection = 'large_calendar'
        filename = 'large_event.ics'
        # Generate a large ICS file content with a single event but a large description
        large_description = "A" * 100000  # 10,000 characters for example
        large_ics_content = f"""BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:uid-single-event@example.com
DTSTAMP:20200714T170000Z
ORGANIZER;CN=User One:MAILTO:{username}@example.com
DTSTART:20200714T170000Z
DTEND:20200714T180000Z
SUMMARY:Large Single Event
DESCRIPTION:{large_description}
END:VEVENT
END:VCALENDAR"""
        # Create a new calendar collection
        self.create_collection(username, password, collection)
        # Add the large ICS file to the collection
        self.add_ics_file(username, password, collection, filename, large_ics_content)
        # Fetch the added ICS file
        response = requests.get(f'{radicale_url}{username}/{collection}/{filename}', auth=(username, password))
        self.assertEqual(response.status_code, 200)
        # Assert the fetched content matches the original large ICS file content
        parsed_response = self.parse_ics(response.text)
        parsed_ics_content = self.parse_ics(large_ics_content)
        self.assertEqual(parsed_response.serialize(), parsed_ics_content.serialize())
        # Clean up by deleting the collection
        self.delete_collection(username, password, collection)

    def test_concurrent_access(self):
        import threading

        def add_event(username, password, collection, filename, content):
            self.add_ics_file(username, password, collection, filename, content)

        username = 'user1'
        password = 'password'
        collection = 'concurrent_access'

        self.create_collection(username, password, collection)

        threads = []
        for i in range(0, 5):  # Create 5 events concurrently
            filename = f'event_{i}.ics'
            content = f"""BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:uid{i}@example.com
DTSTAMP:20200714T170000Z
ORGANIZER;CN=User One:MAILTO:{username}@example.com
DTSTART:20200714T170000Z
DTEND:20200714T180000Z
SUMMARY:Event {i}
END:VEVENT
END:VCALENDAR"""
            thread = threading.Thread(target=add_event, args=(username, password, collection, filename, content))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify all events were added
        for i in range(0, 5):
            filename = f'event_{i}.ics'
            response = requests.get(f'{radicale_url}{username}/{collection}/{filename}', auth=(username, password))
            self.assertEqual(response.status_code, 200)

        self.delete_collection(username, password, collection)

    def test_rapid_create_delete_collections(self):
        username = 'user1'
        password = 'password'
        base_url = f'{radicale_url}{username}/'
        auth = HTTPBasicAuth(username, password)
        headers = {'Content-Type': 'application/xml'}

        for i in range(10):  # Example: Create and delete 10 collections
            collection_name = f'test_collection_{i}'
            collection_url = f'{base_url}{collection_name}/'

            # Create collection
            create_response = requests.request('MKCOL', collection_url, data=CREATE_CALENDAR_XML, auth=auth,
                                               headers=headers)
            self.assertIn(create_response.status_code, [201, 204], f'Failed to create collection {collection_name}')

            # Delete collection
            delete_response = requests.request('DELETE', collection_url, auth=auth)
            self.assertIn(delete_response.status_code, [200, 204, 404],
                          f'Failed to delete collection {collection_name}')

    def test_add_two_events_same_uid(self):
        username = 'user1'
        password = 'password'
        collection = 'test_same_uid'
        filename1 = 'event_same_uid.ics'
        filename2 = 'event_same_uid_2.ics'
        event_content = """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:same_uid@example.com
DTSTAMP:20200714T170000Z
ORGANIZER;CN=User One:MAILTO:user1@example.com
DTSTART:20200714T170000Z
DTEND:20200714T180000Z
SUMMARY:Test Event with Same UID
END:VEVENT
END:VCALENDAR"""

        # Create a new collection
        self.create_collection(username, password, collection)

        self.add_ics_file(username, password, collection, filename1, event_content)
        self.add_ics_file(username, password, collection, filename2, event_content, return_code=[409])

        url = f'{radicale_url}{username}/{collection}/'
        response = requests.request('PROPFIND', url, auth=HTTPBasicAuth(username, password))
        self.assertEqual(response.status_code, 207)

        self.delete_collection(username, password, collection)

    def test_report_filters_text_match_contains(self):
        username = 'user1'
        password = 'password'
        collection = 'test_text_match_contains'
        self.create_collection(username, password, collection)

        # Add events to the collection
        event_contents = ["""BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:unique_event_1@example.com
DTSTAMP:20200714T170000Z
ORGANIZER;CN=User One:MAILTO:user1@example.com
DTSTART:20200714T170000Z
DTEND:20200714T180000Z
SUMMARY:Event with Special Keyword
END:VEVENT
END:VCALENDAR""", """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:unique_event_2@example.com
DTSTAMP:20200715T170000Z
ORGANIZER;CN=User One:MAILTO:user1@example.com
DTSTART:20200715T170000Z
DTEND:20200715T180000Z
SUMMARY:Another Event
END:VEVENT
END:VCALENDAR"""
                          ]

        for i, content in enumerate(event_contents, start=1):
            self.add_ics_file(username, password, collection, f"event_{i}.ics", content)

        # Perform a REPORT request with text-match filter
        report_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop xmlns:D="DAV:">
    <D:getetag/>
    <C:calendar-data/>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="SUMMARY">
          <C:text-match match-type="contains">Special Keyword</C:text-match>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>'''
        headers = {'Content-Type': 'application/xml'}
        url = f'{radicale_url}{username}/{collection}/'
        response = requests.request('REPORT', url, data=report_xml, headers=headers,
                                    auth=HTTPBasicAuth(username, password))

        # Verify that the response contains the event with the specific summary
        self.assertIn("Event with Special Keyword", response.text)
        self.assertNotIn("Another Event", response.text)

        self.delete_collection(username, password, collection)


if __name__ == '__main__':
    unittest.main()
