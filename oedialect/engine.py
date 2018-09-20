import json

import requests
import sqlalchemy
from dateutil.parser import parse as parse_date
from shapely import wkb

from oedialect import error


def date_handler(obj):
    """
    Implements a handler to serialize dates in JSON-strings
    :param obj: An object
    :return: The str method is called (which is the default serializer for JSON) unless the object has an attribute  *isoformat*
    """
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        return str(obj)

class OEConnection():
    """

    """

    """
        Connection methods
    """

    def __init__(self, host='localhost', port=80, user='', database='', password=''):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__token = password
        response = self.post('advanced/connection/open', {})['content']
        self._id = response['connection_id']
        self.__transactions = set()
        self._cursors = set()
        self.__closed = False

    """
        TODO: Look at PGDialect in sqlalchemy.dialects.postgresql.base
    """

    def close(self, *args, **kwargs):
        #for cursor in self._cursors:
        #    cursor.close()
        response = self.post('advanced/connection/close', {},
                             requires_connection_id=True)


    def commit(self, *args, **kwargs):
        response = self.post('advanced/connection/commit', {},
                             requires_connection_id=True)

    def rollback(self, *args, **kwargs):
        response = self.post('advanced/connection/rollback', {},
                             requires_connection_id=True)

    def cursor(self, *args, **kwargs):
        cursor = OECursor(self)
        self._cursors.add(cursor)
        return cursor

    """
        Two-phase commit support methods
    """

    def xid(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_begin(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_commit(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_prepare(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_recover(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_rollback(self, *args, **kwargs):
        raise NotImplementedError

    """
        DB API extension
    """

    def cancel(self, *args, **kwargs):
        raise NotImplementedError

    def reset(self, *args, **kwargs):
        raise NotImplementedError

    def set_session(self, *args, **kwargs):
        raise NotImplementedError

    def set_client_encoding(self, *args, **kwargs):
        raise NotImplementedError

    def set_isolation_level(self, *args, **kwargs):
        raise NotImplementedError

    def get_backend_pid(self, *args, **kwargs):
        raise NotImplementedError

    def get_dsn_parameters(self, *args, **kwargs):
        raise NotImplementedError

    def get_parameter_status(self, *args, **kwargs):
        raise NotImplementedError

    def get_transaction_status(self, *args, **kwargs):
        raise NotImplementedError

    def lobject(self, *args, **kwargs):
        raise NotImplementedError

    """
        Methods related to asynchronous support
    """

    def poll(self, *args, **kwargs):
        raise NotImplementedError

    def fileno(self, *args, **kwargs):
        raise NotImplementedError

    def isexecuting(self, *args, **kwargs):
        raise NotImplementedError


    def post_expect_stream(self, suffix, query, cursor_id=None):
        sender = requests.post

        header = dict(urlheaders)
        header['Authorization'] = 'Token %s' % self.__token

        data = {}
        if cursor_id:
            data['connection_id'] = self._id
            data['cursor_id'] = cursor_id

        response = sender(
            'http://{host}:{port}/api/v0/{suffix}'.format(host=self.__host,
                                                          port=self.__port,
                                                          suffix=suffix),
            json=json.loads(json.dumps(data)),
            headers=header, stream=True)
        try:
            i = 0
            for line in response.iter_lines():
                yield json.loads(line.decode('utf8').replace("'", '"'))
        except Exception as e:
            raise



    def post(self, suffix, query, cursor_id=None, requires_connection_id=False):
        sender = requests.post
        if isinstance(query, dict) and 'request_type' in query:
            if query['request_type'] == 'put':
                sender = requests.put
            if query['request_type'] == 'delete':
                sender = requests.delete

        if 'info_cache' in query:
            del query['info_cache']
        
        data = {'query': query}

        if requires_connection_id or cursor_id:
            data['connection_id'] = self._id

        if cursor_id:
            data['cursor_id'] = cursor_id

        header = dict(urlheaders)
        header['Authorization'] = 'Token %s'%self.__token
        ans = sender(
            'http://{host}:{port}/api/v0/{suffix}'.format(host=self.__host, port=self.__port, suffix=suffix),
            json=json.loads(json.dumps(data, default=date_handler)), headers=header, )

        try:
            json_response = ans.json()
        except:
            raise ConnectionException('Answer contains no JSON: ' + repr(ans))

        if 400 <= ans.status_code < 600:
            raise ConnectionException(json_response['reason'] if 'reason' in json_response else 'No reason returned')

        return json_response

class OECursor:
    description = None
    rowcount = -1

    def __init__(self, connection):
        self.__connection = connection
        try:
            response = self.__connection.post('advanced/cursor/open', {}, requires_connection_id=True)
            if 'content' not in response:
                raise error.CursorError('Could not open cursor: ' + str(response['reason']) if 'reason' in response else 'No reason returned')
            response = response['content']
        except:
            raise
        self.__id = response['cursor_id']


    def __replace_params(self, jsn, params):
        if type(jsn) == dict:
            for k in jsn:
                jsn[k] = self.__replace_params(jsn[k], params)
            return jsn
        elif type(jsn) == list:
            return list(map(lambda x: self.__replace_params(x, params), jsn))
        elif type(jsn) in [str, sqlalchemy.sql.elements.quoted_name,
                           sqlalchemy.sql.elements._truncated_label]:
            return (jsn % params).strip("'<>").replace('\'', '\"')
        elif isinstance(jsn, int):
            return jsn
        elif callable(jsn):
            return jsn(params)
        else:
            raise Exception("Unknown jsn type (%s) in %s" % (type(jsn), jsn))

    __cell_processors = {
        17: lambda cell: wkb.dumps(wkb.loads(cell, hex=True)),
        1114: lambda cell: parse_date(cell),
        1082: lambda cell: parse_date(cell).date()
    }

    def fetchone(self):
        response = self.__connection.post('advanced/cursor/fetch_one', {}, cursor_id=self.__id)[
            'content']
        if response:
            for i, x in enumerate(self.description):
                # Translate WKB-hex to binary representation
                if response[i]:
                    if x[1] in self.__cell_processors:
                        response[i] = self.__cell_processors[x[1]](response[i])
        return response

    def fetchall(self):
        result = self.__connection.post_expect_stream('advanced/cursor/fetch_all', {}, cursor_id=self.__id)
        return result

    def fetchmany(self, size):
        response = self.__connection.post('advanced/cursor/fetch_many', {'size': size}, cursor_id=self.__id)[
            'content']
        return response

    def execute(self, query_obj, params=None):
        if query_obj is None:
            return
        if not isinstance(query_obj, dict):
            if isinstance(query_obj, str):
                raise Exception('Plain string commands are not supported.'
                                'Please use SQLAlchemy datastructures')
            query = query_obj.string
        else:
            query = query_obj
        query = dict(query)
        requires_connection_id = query.get('requires_connection', False)

        query['connection_id'] = self.__connection._id
        query['cursor_id'] = self.__id
        if params:
            query = self.__replace_params(query, params)
        # query = context.compiled.string
        command = query.pop('command')
        return self.__execute_by_post(command, query,
                                  requires_connection_id=requires_connection_id)

    def executemany(self, query, params=None):
        if params is None:
            return self.execute(query)
        else:
            val = None
            for p in params:
                val = self.execute(query, p)
            return val

    def close(self):
        self.__connection.post('advanced/cursor/close', {}, cursor_id=self.__id)

    def __execute_by_post(self, command, query, requires_connection_id=False):

        response = self.__connection.post(command, query, cursor_id=self.__id,
                                requires_connection_id=requires_connection_id)

        if 'content' in response:
            result = response['content']
            if result:
                if isinstance(result, dict):
                    if 'description' in result:
                        self.description = result['description']
                    if 'rowcount' in result:
                        self.rowcount = result['rowcount']
                else:
                    return result
            else:
                return result



urlheaders = {
}


class ConnectionException(Exception):
    pass
