# Copyright 2023 Quip
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""An Object Oriented Quip API library.

For full API documentation, visit https://quip.com/api/.

Typical usage:

    client = quip.QuipClient(access_token=...)
    user = client.get_authenticated_user()
    starred = client.get_folder(user["starred_folder_id"])
    print "There are", len(starred["children"]), "items in your starred folder"

In addition to standard getters and setters, we provide a few convenience
methods for document editing. For example, you can use `add_to_first_list`
to append items (in Markdown) to the first bulleted or checklist in a
given document, which is useful for automating a task list.
"""

import datetime
import json
import ssl
import os, sys
from dotenv import load_dotenv
from types import NoneType
import collections

import urllib.request
import urllib.parse
import urllib.error
from retry import retry

from markdownify import markdownify
import pandas as pd

from io import StringIO
from lxml import etree
#from lxml import html
from bs4 import BeautifulSoup
from typing import Any

Request = urllib.request.Request
urlencode = urllib.parse.urlencode
urlopen = urllib.request.urlopen
HTTPError = urllib.error.HTTPError

try:
    ssl.PROTOCOL_TLSv1_1
    """Configure the environment to accept quip-amazon SSL Certs then return the authenticated client."""
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    raise Exception(
        "Using the Quip API requires an SSL library that supports TLS versions "
        ">= 1.1; your Python + OpenSSL installation must be upgraded.")


class QuipError(Exception):
    def __init__(self, code, message, http_error):
        Exception.__init__(self, "%d: %s" % (code, message))
        self.code = code
        self.http_error = http_error


class Quip(object):
    """Quip Base class"""
    QUIP_BASE_URL = "https://platform.quip-amazon.com"
    REQUEST_TIMEOUT = 10
    access_token = None
    base_url = None

    def __init__(self, access_token: str | None = None, base_url: str | None = None) -> None:
        """Constructs a Quip Base object.
        """
        load_dotenv()
        self.access_token = access_token if access_token else os.getenv('QUIP_ACCESS_TOKEN')
        self.base_url = base_url if base_url else self.QUIP_BASE_URL

    @retry(TimeoutError, tries=3, delay=1, backoff=2)
    def _fetch_json(self, api_version: int, path: str, post_data: Any | None = None, **args) -> Any:
        # for additional arguments such as query= and title= in search queries
        request = Request(url=self._url(api_version, path, **args))
        if post_data:
            post_data = dict((k, v) for k, v in post_data.items()
                             if v or isinstance(v, int))
            request_data = urlencode(self._clean(**post_data))
            request.data = request_data.encode()
        if self.access_token:
            request.add_header("Authorization", "Bearer " + self.access_token)
        try:
            request = urlopen(request, timeout=self.REQUEST_TIMEOUT)
            response_content = json.loads(request.read().decode())
            return request.status, response_content
        except HTTPError as error:
            try:
                # Extract the developer-friendly error message from the response
                message = json.loads(error.read().decode())["error_description"]
            except Exception as e:
                raise error
            raise QuipError(error.code, message, error)

    def _clean(self, **args) -> dict[str, bytes]:
        return dict((k, str(v) if isinstance(v, int) else v.encode("utf-8"))
                    for k, v in args.items() if v or isinstance(v, int))

    def _url(self, api_version: int, path: str, **args) -> str:
        url = self.base_url + f"/{api_version}/" + path
        args = self._clean(**args)
        if args:
            url += "?" + urlencode(args)
        return url


class QuipClient(Quip):
    """A Quip API client"""

    def __init__(self, access_token: str |None = None, base_url: str | None = None) -> None:
        """Constructs a Quip API client.

        If `access_token` is given, all of the API methods in the client
        will work to read and modify Quip documents.
        """
        super().__init__(access_token=access_token, base_url=base_url)

    def get_authenticated_user(self):
        """Returns the QuipUser corresponding to our access token."""
        return QuipUser(client=self)

    def get_user(self, id):
        """Returns the QuipUser with the given ID."""
        return QuipUser(client=self, user_id=id)

    def search(self, title, return_obj=True):
        """Returns a dictionnary of the best matching title and its thread_id."""
        response_status, response_json = self._fetch_json(api_version=1, path="threads/search", query=title, count=1,
                                         only_match_titles=True)
        result = response_json[0].get('thread')

        if result.get('type') == 'document':
            return QuipDocument(access_token=self.access_token, thread_id=result['id'])
        elif result.get('type') == 'spreadsheet':
            return QuipSpreadSheet(access_token=self.access_token, thread_id=result['id'])
        elif result:
            return QuipThread(access_token=self.access_token, thread_id=result['id'])
        else:
            return None

    def advanced_search(self, query, search_content=True, count=None):
        """Returns a dictionary of the document's matching titles and their thread_id."""
        response_status, results_lst = self._fetch_json(api_version=1, path="threads/search", query=query, count=count,
                                       only_match_titles=not search_content)
        search_results_dct = {}
        for result in results_lst:
            search_results_dct.update({result['thread']['title']: result['thread']['id']})
        return search_results_dct


class QuipUser(QuipClient):
    """A QuipUser object"""
    name = None
    emails = None
    id = None
    is_robot = None
    affinity = None
    desktop_folder_id = None
    archive_folder_id = None
    starred_folder_id = None
    private_folder_id = None
    trash_folder_id = None
    shared_folder_ids = None
    group_folder_ids = None
    profile_picture_url = None
    subdomain = None
    url = None
    data = None

    def __init__(self, client=None, access_token=None, base_url=None, user_id=None):
        """Constructs a QuipUser object."""
        if isinstance(client, QuipClient):
            super().__init__(access_token=client.access_token, base_url=client.base_url)
        elif access_token:
            super().__init__(access_token, base_url)
        else:
            return None

        if (user_id == None):
            response_status, response_json = self._fetch_json(api_version=1, path="users/current")
        else:
            response_status, response_json = self._fetch_json(api_version=1, path=f"users/{user_id}")

        self.name = response_json.get('name')
        self.emails = [email.get('address') for email in response_json.get('emails')]
        self.id = response_json.get('id')
        self.desktop_folder_id = response_json.get('desktop_folder_id')
        self.archive_folder_id = response_json.get('archive_folder_id')
        self.starred_folder_id = response_json.get('starred_folder_id')
        self.private_folder_id = response_json.get('private_folder_id')
        self.trash_folder_id = response_json.get('trash_folder_id')
        self.shared_folder_ids = response_json.get('shared_folder_ids')
        self.group_folder_ids = response_json.get('group_folder_ids')
        self.data = response_json


class QuipFolder(QuipClient):
    """A QuipFolder object"""
    # Folder colors
    MANILA, \
        RED, \
        ORANGE, \
        GREEN, \
        BLUE = range(5)
    folder_id = None

    def __init__(self, access_token=None, base_url=None, folder_id=None):
        """Constructs a QuipFolder object.
        """
        super().__init__(access_token, base_url)

    def edit_folder(self, folder_id, color=None,
                    title=None):  # JAYJAY changed update_folder to edit_folder for API consistency
        """Update the folder color / title with the given folder ID."""
        return self._fetch_json(api_version=1, path="folders/update", post_data={
            "folder_id": folder_id,
            "color": color,
            "title": title,
        })

    def delete_folder(self):  # , folder_id): # JAYJAY created the delete_folder function for API CRUD consistency
        """Deletes the folder with the given folder ID."""
        return self._fetch_json(api_version=1, path="folders/delete", post_data={
            "thread_id": folder_id,
        })

    def add_folder_members(self, member_ids):  # , folder_id, member_ids):
        """Adds the given users to the given folder ID."""
        return self._fetch_json(api_version=1, path="folders/add-members", post_data={
            "folder_id": folder_id,
            "member_ids": ",".join(member_ids),
        })

    def remove_folder_members(self, folder_id, member_ids):
        """Removes the given users from the given folder ID."""
        return self._fetch_json(api_version=1, path="folders/remove-members", post_data={
            "folder_id": folder_id,
            "member_ids": ",".join(member_ids),
        })


class QuipThread(Quip):
    """A QuipThread Parent object"""
    # Edit locations
    AFTER_SECTION = 2
    BEFORE_SECTION = 3
    REPLACE_SECTION = 4
    DELETE_SECTION = 5

    id = None
    title = None
    type = None
    link = None
    secret_path = None
    author_id = None
    created_usec = None
    updated_usec = None

    metadata = None
    content_html = None
    tree = None
    folders = None

    def __init__(self, client=None, access_token=None, base_url=None, thread_id=None,
                 format="html", title=None, content=" ", member_ids=[], doc_type="document"):
        """Constructs a QuipThread object.
            Creates a new document from the given content.
                client = QuipClient(...)
                # Get
                thread = QuipThread(client = QuipClient(), or Access Token

                # Or Create
                thread = QuipThread(client = QuipClient() | access_token = String,
                                    content = Required string("The HTML or Markdown content of the new document"),
                                    format = Default "html" | "mardown",
                                    title = String, if not specified inferred from heading.
                                    member_ids = List( user_IDs, folder_ID(s) ), default to authenticated user & Private folder.
                                    doc_type = Default "document" | "spreadshet")

            NOTE: The document will be placed in the specified folder(s), and any individual users listed will
                  be granted individual access to the document. If this argument is not given, the document
                  is created in the authenticated user's Private folder.
                  """
        if isinstance(client, QuipClient):
            super().__init__(access_token=client.access_token, base_url=client.base_url)
        else:
            super().__init__(access_token, base_url)

        if (thread_id == None):
            response_status, response_json = self._fetch_json(api_version=1, path="threads/new-document",
                                             post_data={
                                                 "content": content,
                                                 "format": format,
                                                 "title": title,
                                                 "member_ids": ",".join(member_ids),
                                                 "type": doc_type
                                             })
        else:
            response_status, response_json = self._fetch_json(api_version=2, path=f"threads/{thread_id}")

        thread_json = response_json.get('thread')
        self.id = thread_json.get('id')
        self.title = thread_json.get('title')
        self.type = thread_json.get('type')
        self.link = thread_json.get('link')
        self.secret_path = thread_json.get('secret_path')
        self.author_id = thread_json.get('author_id')
        self.created_usec = thread_json.get('created_usec')
        self.updated_usec = thread_json.get('updated_usec')
        self.metadata = response_json
        self.content_html = self._get_thread_html()
        try:
            self.content_tree = etree.parse(StringIO(self.content_html), etree.HTMLParser())
        except Exception as e:
            print("Quip File corrupted, closing html tab without opening, some methods of the QuipSpreadSheet class may not work")
        self.folder_ids = self._get_folder_ids()

    def _get_thread_html(self):
        """Returns the html content of the thread.
           https://quip.com/dev/automation/documentation/current#operation/getThreadHtmlV2
           """
        html_content = ''
        next_cursor = None
        while (next_cursor != ''):
            response_status, response_json = self._fetch_json(api_version=2, path=f"threads/{self.id}/html", cursor=next_cursor)
            html_content = html_content + response_json.get('html')
            next_cursor = response_json['response_metadata'].get('next_cursor')
        return "<html>" + html_content + "</html>" if len(html_content) > 0 else None

    def _get_folder_ids(self):
        """Returns the folders of the thread.
           https://quip.com/dev/automation/documentation/current#operation/getThreadFolders
           """
        folder_ids = []
        next_cursor = None
        while (next_cursor != ''):
            response_status, response_json = self._fetch_json(api_version=2, path=f"threads/{self.id}/folders", cursor=next_cursor)
            for folder in response_json.get('folders'):
                folder_ids.append(folder.get('folder_id'))
            next_cursor = response_json['response_metadata'].get('next_cursor')
        self.folder_ids = folder_ids if len(folder_ids) > 0 else None
        return folder_ids  # JAYJAY TODO: we should store folder_ids but Folders objects will have to be instanciated

    def _link(self, destination_folder_id):
        """Moves the given thread from the source folder to the destination one.
        """
        self._add_thread_members(self.id, destination_folder_id)

    def move(self, destination_folder_id):
        """Moves the given thread from the source folder to the destination one.
        """
        source_folder_id = self._get_folders()
        self._link_thread(destination_folder_id)
        self._remove_thread_members(self.id, source_folder_id)

    def _copy(self, folder_ids=None, member_ids=None, title=None, copy_annotations=False):
        """Copies the given document, returns QuipThread object.
        """
        args = {"thread_id": self.id}
        if folder_ids:
            args["folder_ids"] = ",".join(folder_ids)
        if member_ids:
            args["member_ids"] = ",".join(member_ids)
        if not title:
            args["title"] = self.title + ' (Copy)'
        args["copy_annotations"] = copy_annotations
        response_status, response_json = self._fetch_json(api_version=1, path="threads/copy-document", post_data=args)
        result = response_json.get('thread')
        return QuipThread(access_token=self.access_token, thread_id=result['id']) if result else None

    def edit_thread(self, content, location=None, format="markdown",
                      section_id=None):
        """Edits the given document, adding the given content.
        `location` should be one of the constants described above. If
        `location` is relative to another section of the document, you must
        also specify the `section_id`.
        """

        # Since our cell ids in 10x contain ';', which is a valid cgi
        # parameter separator, we are replacing them with '_' in 10x cell
        # sections. This should be no op for all other sections.
        section_id = None if not section_id else section_id.replace(";", "_")

        args = {
            "thread_id": self.id,
            "content": content,
            "location": location,
            "format": format,
            "section_id": section_id
        }
        return self._fetch_json(api_version=1, path="threads/edit-document", post_data=args)

    def delete(self):
        """Deletes the thread with the given thread id or secret"""
        return self._fetch_json(api_version=1, path="threads/delete", post_data={
            "thread_id": self.id,
        })

    def add_thread_members(self, member_ids):
        """Adds the given folder or user IDs to the given thread."""
        if isinstance(member_ids, str):
            member_ids = [member_ids]
        return self._fetch_json(api_version=1, path="threads/add-members", post_data={
            "thread_id": self.id,
            "member_ids": ",".join(member_ids),
        })

    def remove_thread_members(self, member_ids):
        """Removes the given folder or user IDs from the given thread."""
        if isinstance(member_ids, str):
            member_ids = [member_ids]
        return self._fetch_json(api_version=1, path="threads/remove-members", post_data={
            "thread_id": self.id,
            "member_ids": ",".join(member_ids),
        })

    def _parse_micros(self, usec):  # JAYJAY DEAD CODE
        """Returns a `datetime` for the given microsecond string"""
        return datetime.datetime.utcfromtimestamp(usec / 1000000.0)

    def _get_blob(self, thread_id, blob_id):
        """Returns a file-like object with the contents of the given blob from
        the given thread.

        The object is described in detail here:
        https://docs.python.org/2/library/urllib2.html#urllib2.urlopen
        """
        request = Request(
            url=self._url("blob/%s/%s" % (thread_id, blob_id)))
        if self.access_token:
            request.add_header("Authorization", "Bearer " + self.access_token)
        try:
            return urlopen(request, timeout=self.REQUEST_TIMEOUT)
        except HTTPError as error:
            try:
                # Extract the developer-friendly error message from the response
                message = json.loads(error.read().decode())["error_description"]
            except Exception:
                raise error
            raise QuipError(error.code, message, error)

    def _put_blob(self, thread_id, blob, name=None):
        """Uploads an image or other blob to the given Quip thread. Returns an
        ID that can be used to add the image to the document of the thread.

        blob can be any file-like object. Requires the 'requests' module.
        """
        import requests
        url = "blob/" + thread_id
        headers = None
        if self.access_token:
            headers = {"Authorization": "Bearer " + self.access_token}
        if name:
            blob = (name, blob)
        try:
            response = requests.request(
                "post", self._url(url), timeout=self.REQUEST_TIMEOUT,
                files={"blob": blob}, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            try:
                # Extract the developer-friendly error message from the response
                message = error.response.json()["error_description"]
            except Exception:
                raise error
            raise QuipError(error.response.status_code, message, error)


class QuipDocument(QuipThread):
    """A Quip Document object"""
    # Edit locations
    AFTER_DOC = 0
    BEFORE_DOC = 1

    AFTER_RANGE = 6
    BEFORE_RANGE = 7
    REPLACE_RANGE = 8
    DELETE_RANGE = 9

    content_markdown = None

    def __init__(self, client=None, access_token=None, base_url=None, thread_id=None,
                 format="markdown", title=None, content=" ", member_ids=[]):  # format="html"|"markdown"
        """Constructs a QuipDocument object.
        """
        super().__init__(client=client, access_token=access_token, base_url=base_url, thread_id=thread_id,
                         format=format, title=title, content=content, member_ids=member_ids, doc_type="document")
        self.content_markdown = markdownify(self.content_html)

    def copy(self, folder_ids=None, member_ids=None, title=None, copy_annotations=False):
        """Copies the given document, returns either a dictionnary {title:..,thread_id} or a QuipDocument object.
        """
        thread_copy = super._copy(folder_ids=folder_ids, member_ids=member_ids, title=title,
                                  copy_annotations=copy_annotations)
        return QuipDocument(access_token=self.access_token, thread_id=thread_copy.get('id')) if thread_copy else None

    def edit_range(self, content, location=None, format="markdown",
                   document_range=None):
        """Edits the given range, adding the given content.
        `location` should be one of the constants described above. If
        `location` is relative to another section of the document, you must
        also specify the `document_range`.
        """

        # Since our cell ids in 10x contain ';', which is a valid cgi
        # parameter separator, we are replacing them with '_' in 10x cell
        # sections. This should be no op for all other sections.
        document_range = None if not document_range else document_range.replace(";", "_")

        args = {
            "thread_id": self.id,
            "content": content,
            "location": location,
            "format": format,
            "document_range": document_range
        }
        return self._fetch_json(api_version=1, path="threads/edit-document", post_data=args)

    def _get_element_tree(self):
        df = pd.DataFrame()
        for child in self.content_tree.iter():
            df = pd.concat([df, pd.DataFrame({'tag': [child.tag], 'text': [child.text], 'attrib': [child.attrib]},
                                             index=[child.attrib.get('id') if child.attrib.get('id') else 0])])
        return df

    def _get_section_elementTree(self, section_id):
        element = list(self.content_tree.iterfind(".//*[@id='%s']" % section_id))
        if not element:
            return None
        return element[0]

    def _get_list_item_section_ids(self, section_id):
        """Like `get_last_list_item_id`, but the first item in the list."""
        list_tree = self._get_section_elementTree(section_id)
        return list(list_tree.iter("li"))

    def _get_first_list_item_section_id(self, section_id):
        items = self._get_list_item_section_ids(section_id)
        return items[0].attrib["id"] if items else None

    def _get_last_list_item_section_id(self, section_id):
        """Returns the last item in the given list `ElementTree`."""
        items = self._get_list_item_section_ids(section_id)
        return items[-1].attrib["id"] if items else None

    def _get_nth_list_item_section_id(self, list_section_id=None, idx=None):
        items = self._get_list_item_section_ids(list_section_id)
        return items[idx].attrib["id"] if items else None

    def content_add_after_document(self, content=None, format="markdown"):
        if content == None:
            raise Exception("content is required for content_add_after_document")
        return self.doc_edit_content(content, location=self.AFTER_DOC, format=format, section_id=None)

    def content_add_before_document(self, content=None, format="markdown"):
        if content == None:
            raise Exception("content is required for content_add_before_document")
        return self.doc_edit_content(content, location=self.BEFORE_DOC, format=format, section_id=None)

    def content_add_after_section(self, content=None, format="markdown", section_id=None):
        if (content == None) | (section_id == None):
            raise Exception("content and section_id are required for content_add_after_section")
        return self.doc_edit_content(content, location=self.AFTER_SECTION, format=format, section_id=section_id)

    def content_add_before_section(self, content=None, format="markdown", section_id=None):
        if (content == None) | (section_id == None):
            raise Exception("content and section_id are required for content_add_before_section")
        return self.doc_edit_content(content, location=self.BEFORE_SECTION, format=format, section_id=section_id)

    def content_replace_section(self, content=None, format="markdown", section_id=None):
        if (content == None) | (section_id == None):
            raise Exception("content and section_id are required for content_replace_section")
        return self.doc_edit_content(content, location=self.REPLACE_SECTION, format=format, section_id=section_id)

    def content_delete_section(self,
                               section_id=None):  # TODO JAYJAY To be Tested as the doc says it's not possible? except by replacing a section by '' (just check if DELETE_SECTION works or do we need REPLACE_SECTION)
        if section_id == None:
            raise Exception("section_id is required for delete_section")
        return self.doc_edit_content(content=' ', location=self.DELETE_SECTION, format="markdown", section_id=section_id)

    def _get_headers_section_ids(self):  # TODO JAYJAY Not Used Anywhere????
        df = self._get_element_tree()
        headers_df = df[df['tag'].str.contains(pat='h\d', regex=True)]
        headers_df.reset_index(inplace=True)
        headers_df = headers_df[['tag', 'text', 'index']]
        headers_df.columns = ['tag', 'text', 'section_id']
        return headers_df[headers_df['section_id'] != 0]

    def content_add_after_range(self, content=None, format="markdown", header=None):
        if (content == None) | (header == None):
            raise Exception("content and header are required for content_add_after_range")
        return self.edit_range(content, location=self.AFTER_RANGE, format=format, document_range=header)

    def content_add_before_range(self, content=None, format="markdown", header=None):
        if (content == None) | (header == None):
            raise Exception("content and header are required for content_add_before_range")
        return self.edit_range(content, location=self.BEFORE_RANGE, format=format, document_range=header)

    def content_replace_range(self, content=None, format="markdown", header=None):
        if (content == None) | (header == None):
            raise Exception("content and header_section_id are required for content_replace_range")
        return self.edit_range(content, location=self.REPLACE_RANGE, format=format, document_range=header)

    def content_delete_range(self,
                             header=None):  # TODO JAYJAY To be Tested as the doc says it's not possible? except by replacing a section by '' (just check if DELETE_SECTION works or do we need REPLACE_SECTION)
        if (header == None):
            raise Exception("<header> string is required for content_delete_range")
        return self.edit_range(content=' ', location=self.DELETE_RANGE, document_range=header)

    def _get_lists_section_ids(self):  # TODO JAYJAY Not Used Anywhere????
        df = self._get_element_tree()
        lists_df = df[df['tag'].str.contains(pat='^ul|^ol', regex=True)]
        lists_df.reset_index(inplace=True)
        lists_df = lists_df[['tag', 'index']]
        lists_df.columns = ['tag', 'section_id']
        return lists_df[lists_df['section_id'] != 0]

    def content_add_after_list(self, content=None, format="markdown", list_section_id=None):
        if (content == None) | (list_section_id == None):
            raise Exception("content and list section_id are required for content_add_after_section")
        section_id = self._get_last_list_item_section_id(list_section_id)
        return self.doc_edit_content(content, location=self.AFTER_SECTION, format=format, section_id=section_id)

    def content_add_before_list(self, content=None, format="markdown", list_section_id=None):
        if (content == None) | (list_section_id == None):
            raise Exception("content and list section_id are required for content_add_after_section")
        section_id = self._get_first_list_item_section_id(list_section_id)
        return self.doc_edit_content(content, location=self.BEFORE_SECTION, format=format, section_id=section_id)

    def content_insert_after_list_item(self, content=None, format="markdown", list_section_id=None, item_idx=None):
        if (content == None) | (list_section_id == None) | (item_idx == None):
            raise Exception("content, list section_id and item_idx are required for content_insert_after_list_item")
        section_id = self._get_nth_list_item_section_id(list_section_id, item_idx)
        return self.doc_edit_content(content, location=self.AFTER_SECTION, format=format, section_id=section_id)

    def content_replace_list_item(self, content=None, format="markdown", list_section_id=None, item_idx=None):
        if (content == None) | (list_section_id == None) | (item_idx == None):
            raise Exception("content, list section_id and item_idx are required for content_replace_list_item")
        section_id = self._get_nth_list_item_section_id(list_section_id, item_idx)
        return self.doc_edit_content(content, location=self.REPLACE_SECTION, format=format, section_id=section_id)

    def doc_edit_content(self, content, location=None, format="markdown", section_id=None) -> str: # HTTPResponse.status
        response_status, response_content = self.edit_thread(content, location=location, format=format, section_id=section_id)
        if isinstance(response_content, dict):
            self.reload_content(response_content.get('html'))
        return response_status

    def reload_content(self, html=None):
        if html:
            self.content_html = html
        else:
            self.content_html = self._get_thread_html()

        try:
            parser = etree.HTMLParser()
            self.content_tree = etree.parse(StringIO(self.content_html), parser)
        except Exception as e:
            print(f"Quip thread is corrupted: {e}")
            self.content_tree = None
        self.content_markdown = markdownify(self.content_html)
        return



class QuipSpreadSheet(QuipThread):
    """A Quip Spreadsheet object
    A Spreadsheet is a Quip document
    A Sheet is tab within a spreadsheet

    NOTE: When creating a spreadsheet, the content must be surrounded by an HTML <table> tag.
          In the current implementation, the parser treats <th> tags as <td> tags.
          When more than one <tr> tag is supplied, then the first row is automatically interpreted
          as the column header. If you want to create a table with more than one row and have default
          headers, then provide the first row with empty <td> tags.
    """

    content_sheet_json = None
    content_sheet_dataframes = None
    sheet_names = None

    def __init__(self, client=None, access_token=None, base_url=None, thread_id=None,
                 title=None, content=None, member_ids=[]):
        """Constructs a QuipSpreadsheet object."""
        content_html = content.to_html(index=False) if isinstance(content, pd.DataFrame) else None
        super().__init__(client=client, access_token=access_token, base_url=base_url, thread_id=thread_id,
                         format="html", title=title, content=content_html, member_ids=member_ids,
                         doc_type="spreadsheet")
        self.content_sheet_dataframes = self._get_sheets_as_dataframes()
        self.sheet_names = list(self.content_sheet_dataframes.keys())
        self.content_sheet_json = self._get_sheets_as_json()

    def _get_sheets_as_dataframes(self):
        soup = BeautifulSoup(self.content_html, 'html.parser')
        table_names_lst = [t.attrs['title'] for t in soup.select('table[title]')]
        df_lst = pd.read_html(StringIO(self.content_html), index_col=0, flavor='bs4')
        spreadsheet_dct = {}
        for name, df in zip(table_names_lst, df_lst):
            spreadsheet_dct.update({name: df})
        return spreadsheet_dct

    def _get_sheets_as_json(self):
        soup = BeautifulSoup(self.content_html, 'html.parser')
        table_names_lst = [t.attrs['title'] for t in soup.select('table[title]')]
        spreadsheet_dct = {}
        for name in table_names_lst:
            spreadsheet_dct.update({name: self.parse_sheet_contents(name)})
        return spreadsheet_dct

    def _sheet_name_to_tree(self, sheet_name=None):
        if sheet_name:
            element = list(self.content_tree.iterfind(".//*[@title='%s']" % sheet_name))
            if not element:
                return None
            return element[0]
        else:
            lists = list(self.content_tree.iter("table"))
            if not lists:
                return None
            else:
                return lists[0]  # get_first_sheet()

    def _list_to_html(self, *rows):
        return "".join(["<tr>%s</tr>" % "".join(["<td>%s</td>" % cell for cell in row]) for row in rows])

    def _dict_to_html(self, updates_dct=None, sheet_name=None):
        sheet_tree = self._sheet_name_to_tree(sheet_name)
        headers = self._get_sheet_tree_col_names(sheet_tree)[1:]
        indexed_items = {}
        extra_items = []
        for head, val in updates_dct.items():
            index = self._get_col_name_index(
                headers, head, default=None)
            if index is None or index in indexed_items:
                extra_items.append(val)
            else:
                indexed_items[index] = val
        cells = []
        if indexed_items:
            for i in range(max(indexed_items.keys()) + 1):
                if i in indexed_items:
                    cells.append(indexed_items[i])
                elif len(extra_items):
                    cells.append(extra_items.pop(0))
                else:
                    cells.append("")
        cells.extend(extra_items)
        return "<tr>%s</tr>" % "".join(
            ["<td>%s</td>" % cell for cell in cells])

    def _get_nth_row_section_id(self, sheet_tree, idx=None):
        """Returns the nth row in the given spreadsheet `ElementTree`."""
        items = list(sheet_tree.iter("tr"))
        return items[idx].attrib["id"] if items else None

    def _sheet_add_row(self, row_update, sheet_name=None, row_idx=None, location=None):
        """Adds the given rows to the named (or first) spreadsheet in the
        given document.
            client = quip.QuipClient(...)
            client._sheet_add_row(["5/1/2014", 2.24])
        """
        sheet_tree = self._sheet_name_to_tree(sheet_name)

        if isinstance(row_idx, NoneType):
            if (location == self.BEFORE_SECTION):
                section_id = self._get_nth_row_section_id(sheet_tree, 1)
                location = self.BEFORE_SECTION
            else:
                section_id = self._get_nth_row_section_id(sheet_tree, -1)
                location = self.AFTER_SECTION
        else:
            section_id = self._get_nth_row_section_id(sheet_tree, row_idx)
            location = self.AFTER_SECTION if isinstance(location, NoneType) else location

        if isinstance(row_update, list):
            content = self._list_to_html(row_update)
        elif isinstance(row_update, dict):
            content = self._dict_to_html(row_update)
        else:
            return None
        return self.sheet_edit_content(
            content=content,
            format="html",
            section_id=section_id,
            location=location)

    def sheet_row_prepend(self, *rows, sheet_name=None):
        return self._sheet_add_row(*rows, sheet_name=sheet_name, location=self.BEFORE_SECTION)

    def sheet_row_append(self, *rows, sheet_name=None):
        return self._sheet_add_row(*rows, sheet_name=sheet_name, location=self.AFTER_SECTION)

    def sheet_row_insert_before(self, *rows, sheet_name=None, row_idx=None):
        return self._sheet_add_row(*rows, sheet_name=sheet_name, row_idx=row_idx, location=self.BEFORE_SECTION)

    def sheet_row_insert_after(self, *rows, sheet_name=None, row_idx=None):
        return self._sheet_add_row(*rows, sheet_name=sheet_name, row_idx=row_idx, location=self.AFTER_SECTION)

    def _get_nth_row_tree(self, sheet_tree, idx=None):
        """Returns the nth row in the given spreadsheet `ElementTree`."""
        items = list(sheet_tree.iter("tr"))
        return items[idx]

    def get_row_values(self, row_idx):
        """Returns the text of items in the given row index."""
        row_tree = self._get_nth_row_tree(row_idx)
        return [(list(x.itertext()) or [None])[0] for x in row_tree]

    def _get_row_tree_values(self, row_tree):
        """Returns the text of items in the given row `ElementTree`."""
        return [(list(x.itertext()) or [None])[0] for x in row_tree]

    def get_sheet_col_names(self, sheet_name):
        """Returns the header row in the given sheet name."""
        sheet_tree = self._sheet_name_to_tree(sheet_name)
        return self._get_row_tree_values(list(sheet_tree.iterfind(".//tr"))[0])

    def _get_sheet_tree_col_names(self, sheet_tree):
        """Returns the header row in the given spreadsheet `ElementTree`."""
        return self._get_row_tree_values(list(sheet_tree.iterfind(".//tr"))[0])

    def _get_col_name_index(self, header_items, header, default=0):
        """Find the index of the given header in the items"""
        if header:
            header = str(header)
            lower_headers = [str(h).lower() for h in header_items]
            if header in header_items:
                return header_items.index(header)
            elif header.lower() in lower_headers:
                return lower_headers.index(header.lower())
            elif header.isdigit():
                return int(header)
            elif len(header) == 1:
                char = ord(header.upper())
                if ord('A') < char < ord('Z'):
                    return char - ord('A') + 1
            else:
                pass
        return default

    def _find_row_tree(self, sheet_tree, header, value):  # TODO JAYJAY KEEP IT <input sheet_name, output tree>
        """Find the row in the given spreadsheet `ElementTree` where header is value."""
        headers = self._get_sheet_tree_col_names(sheet_tree)
        index = self._get_col_name_index(headers, header)
        for row in sheet_tree.iterfind(".//tr"):
            if len(row) <= index:
                continue
            cell = row[index]
            if cell.tag != "td":
                continue
            if list(cell.itertext())[0].lower() == value.lower():
                return row

    def _get_row_section_ids(self, row_tree):
        """Returns a lsit of section_ids in the given row `ElementTree`."""
        return [x.attrib.get("id", "") for x in row_tree]

    def sheet_search_update_cells(self, sheet_name, search_dct, updates_dct):
        """Finds the row where the given header column is the given value, and
        applies the given updates. Updates is a dict from header to
        new value. In both cases headers can either be a string that matches, or
        "A", "B", "C", 1, 2, 3 etc. If no row is found, adds a new one with searched value and updates.
            QuipSpreadSheet.update_spreadsheet_cells(sheet_name"Table1", search_dct={"customer":"Acme"}, updates_dct={"Billed": "6/24/2015"})
        """
        response = None
        header = list(search_dct.keys())[0]
        value = search_dct.get(header)
        sheet_tree = self._sheet_name_to_tree(sheet_name)
        headers = self._get_sheet_tree_col_names(sheet_tree)
        row_tree = self._find_row_tree(sheet_tree, header, value)
        if row_tree:
            section_ids = self._get_row_section_ids(row_tree)
            for head, val in updates_dct.items():  # TODO JAYJAY iteritems() on a dict
                index = self._get_col_name_index(headers, head)
                if not index or index >= len(section_ids) or not section_ids[index]:
                    continue
                response = self.sheet_edit_content(
                    content=val,
                    format="markdown",
                    section_id=section_ids[index],
                    location=self.REPLACE_SECTION
                )
        else:
            updates_dct[header] = value
            response = self._sheet_add_row(updates_dct, sheet_name)
        return response

    def sheet_update_cells(self, sheet_name=None, col_name_row_idx=None,
                           update_val=None):  # TODO: JAYJAY Code Error cases (col_name not found, row_idx not found, etc.)
        """"A", "B", "C", 1, 2, 3 etc. If no row is found, adds a new one.
            QuipSpreadSheet.sheet_update_cell(sheet_name="Table1", "A:8", "Billed"})
        """
        col_name_row_idx = str.split(col_name_row_idx, ":")
        col_name = col_name_row_idx[0]
        row_idx = int(col_name_row_idx[1])
        section_id = self.content_sheet_json[sheet_name]['rows'][row_idx-1]['cells'][col_name]['id']
        return self.sheet_edit_content(
            content=update_val,
            format="markdown",
            section_id=section_id, #s[index],
            location=self.REPLACE_SECTION
        )

    def sheet_upload_dataframe(self, sheet_name=None, df=None):
        sheet_tree = self._sheet_name_to_tree(sheet_name)
        row_lst = [df.columns.to_list()]
        for idx, row_df in df.iterrows():
            row_lst.extend([row_df.to_list()])
        content = "".join(["<tr>%s</tr>" % "".join(["<td>%s</td>" % cell for cell in row]) for row in row_lst])
        response =  self.sheet_edit_content(content=content,
                                  section_id=self._get_nth_row_section_id(sheet_tree, 1),
                                  location=self.BEFORE_SECTION)
        return response

    def parse_sheet_contents(self, sheet_name=None):
        """Returns a python-friendly representation of the given sheet `ElementTree`"""
        sheet_tree = self._sheet_name_to_tree(sheet_name)
        sheet = {
            "id": sheet_tree.attrib.get("id"),
            "headers": self._get_sheet_tree_col_names(sheet_tree),
            "rows": [],
        }
        for row in sheet_tree.iterfind(".//tr"):
            value = {
                "id": row.attrib.get("id"),
                "cells": collections.OrderedDict(),
            }
            for i, cell in enumerate(row):
                if cell.tag != "td":
                    continue
                data = {
                    "id": cell.attrib.get("id"),
                }
                images = list(cell.iter("img"))
                if images:
                    data["content"] = images[0].attrib.get("src")
                else:
                    data["content"] = list(cell.itertext())[0].replace(
                        u"\u200b", "")
                style = cell.attrib.get("style")
                if style and "background-color:#" in style:
                    sharp = style.find("#")
                    data["color"] = style[sharp + 1:sharp + 7]
                value["cells"][sheet["headers"][i]] = data
            if len(value["cells"]):
                sheet["rows"].append(value)
        return sheet

    def export_sheet_as_excel(self, sheet_name=None):
        if sheet_name in self.sheet_names:
            filename = f"{self.title}_{sheet_name}.xlsx"
            self.content_sheet_dataframes.get(sheet_name).to_excel(filename)
            print(f"'{filename}' written for sheet:'{sheet_name}'")
        return

    def sheet_edit_content(self, content, location=None, format="markdown", section_id=None) -> str: # HTTPResponse.status
        response_status, response_content = self.edit_thread(content, location=location, format=format, section_id=section_id)
        if isinstance(response_content, dict):
            self.reload_content(response_content.get('html'))
        return response_status

    def reload_content(self, html=None):
        if html:
            self.content_html = html
        else:
            self.content_html = self._get_thread_html()

        try:
            parser = etree.HTMLParser()
            self.content_tree = etree.parse(StringIO(self.content_html), parser)
        except Exception as e:
            print(f"Quip thread is corrupted: {e}")
            self.content_tree = None

        self.sheet_names = list(self.content_sheet_dataframes.keys())
        self.content_sheet_dataframes = self._get_sheets_as_dataframes()
        self.content_sheet_json = self._get_sheets_as_json()
        return

# TODO LIST

# DOCUMENT add a method to get the content as a friendly JSON representation ???? (THAT WOULD BE AWESOME)
# Also check if there are fancy trick based on the new HTML parser (as opposed to the old XLM parser)

# SPREADSHEET replace xml iterations by json indexing (simpler to read and more consize)
# Need add comments (Copilot will do)
# Need to implement typing
# Need to make a Notebook with examples for all methods as a README.md
