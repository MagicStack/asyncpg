cdef enum ConnectionStatus:
    CONNECTION_OK = 0
    CONNECTION_BAD = 1
    CONNECTION_STARTED = 2           # Waiting for connection to be made.
    CONNECTION_MADE = 3              # Connection OK; waiting to send.
    CONNECTION_AWAITING_RESPONSE = 4 # Waiting for a response from the
                                     # postmaster.
    CONNECTION_AUTH_OK = 5           # Received authentication; waiting for
                                     # backend startup.
    CONNECTION_SETENV = 6            # Negotiating environment.
    CONNECTION_SSL_STARTUP = 7       # Negotiating SSL.
    CONNECTION_NEEDED = 8            # Internal state: connect() needed

cdef enum AsyncStatus:
    # defines the state of the query-execution state machine
    PGASYNC_IDLE = 0                 # nothing's happening, dude
    PGASYNC_BUSY = 1                 # query in progress
    PGASYNC_READY = 2                # result ready for PQgetResult
    PGASYNC_COPY_IN = 3              # Copy In data transfer in progress
    PGASYNC_COPY_OUT = 4             # Copy Out data transfer in progress
    PGASYNC_COPY_BOTH = 5            # Copy In/Out data transfer in progress

cdef enum QueryClass:
    # tracks which query protocol we are now executing
    PGQUERY_SIMPLE = 0               # simple Query protocol (PQexec)
    PGQUERY_EXTENDED = 1             # full Extended protocol (PQexecParams)
    PGQUERY_PREPARE = 2              # Parse only (PQprepare)
    PGQUERY_DESCRIBE = 3             # Describe Statement or Portal
    PGQUERY_CLOSE = 4

cdef enum ExecStatusType:
    PGRES_EMPTY_QUERY = 0            # empty query string was executed
    PGRES_COMMAND_OK = 1             # a query command that doesn't return
                                     # anything was executed properly by the
                                     # backend
    PGRES_TUPLES_OK = 2              # a query command that returns tuples was
                                     # executed properly by the backend,
                                     # PGresult contains the result tuples
    PGRES_COPY_OUT = 3               # Copy Out data transfer in progress
    PGRES_COPY_IN = 4                # Copy In data transfer in progress
    PGRES_BAD_RESPONSE = 5           # an unexpected response was recv'd from
                                     # the backend
    PGRES_NONFATAL_ERROR = 6         # notice or warning message
    PGRES_FATAL_ERROR = 7            # query failed
    PGRES_COPY_BOTH = 8              # Copy In/Out data transfer in progress
    PGRES_SINGLE_TUPLE = 9           # single tuple from larger resultset

cdef enum TransactionStatus:
    PQTRANS_IDLE = 0                 # connection idle
    PQTRANS_ACTIVE = 1               # command in progress
    PQTRANS_INTRANS = 2              # idle, within transaction block
    PQTRANS_INERROR = 3              # idle, within failed transaction
    PQTRANS_UNKNOWN = 4              # cannot determine status

cdef enum MessageDispatchLoop:
    DISPATCH_CONTINUE = 0
    DISPATCH_STOP = 1


cdef class Result:
    cdef:
        ExecStatusType status

        # message broken into fields
        dict err_fields
        # text of triggering query, if available
        str  err_query

        # cmd status from the query
        bytes cmd_status

        object parameters_desc
        object row_desc
        list rows

    @staticmethod
    cdef Result new(ExecStatusType status)


cdef class CoreProtocol:
    cdef:
        object transport
        ReadBuffer buffer

        ####### Options:

        str _user
        str _password
        str _dbname
        str _encoding

        ####### Connection State:
        int _backend_pid
        int _backend_secret

        # result being constructed
        Result            _result

        ConnectionStatus  _status
        AsyncStatus       _async_status
        QueryClass        _query_class
        TransactionStatus _xact_status

        WriteBuffer _after_sync

    cdef inline _write(self, WriteBuffer buf)
    cdef inline _write_sync_message(self)
    cdef inline _read_server_messages(self)
    cdef inline MessageDispatchLoop _dispatch_server_message(self, char mtype)
    cdef _parse_server_authentication(self)
    cdef _parse_server_parameter_status(self)
    cdef _parse_server_backend_key_data(self)
    cdef _parse_server_parameter_description(self)
    cdef _parse_server_ready_for_query(self)
    cdef _parse_server_row_description(self)
    cdef _parse_server_data_row(self)
    cdef _parse_server_error_response(self, is_error)
    cdef _fatal_error(self, exc)
    cdef _push_result(self)
    cdef _sync(self)
    cdef _ensure_ready_state(self)

    cdef _connect(self)
    cdef _query(self, str query)
    cdef _parse(self, str stmt_name, str query)
    cdef _bind(self, str portal_name, str stmt_name, WriteBuffer bind_data)
    cdef _describe(self, str name, bint is_portal)
    cdef _close(self, str name, bint is_portal)

    cdef _on_result(self, Result result)
    cdef _on_fatal_error(self, exc)
    cdef _on_connection_lost(self, exc)
    cdef _set_server_parameter(self, key, val)
