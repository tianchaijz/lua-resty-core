-- Copyright (C) Yichun Zhang (agentzh)
-- Copyright (C) Jinzheng Zhang (tianchaijz)
-- I hereby assign copyright in this code to the lua-resty-core project,
-- to be licensed under the same terms as the rest of the code.


local base = require "resty.core.base"
base.allows_subsystem("http", "stream")


local ffi = require "ffi"
local FFI_OK = base.FFI_OK
local FFI_ERROR = base.FFI_ERROR
local FFI_DECLINED = base.FFI_DECLINED
local ffi_new = ffi.new
local ffi_str = ffi.string
local ffi_gc = ffi.gc
local C = ffi.C
local type = type
local error = error
local tonumber = tonumber
local get_request = base.get_request
local get_string_buf = base.get_string_buf
local get_size_ptr = base.get_size_ptr
local setmetatable = setmetatable
local co_yield = coroutine._yield
local subsystem = ngx.config.subsystem
local ngx_sleep = ngx.sleep

local ERR_BUF_SIZE = 128
local MSG_BUF_SIZE = 32 * 1024


local errmsg = base.get_errmsg_ptr()
local msg_buf = get_string_buf(MSG_BUF_SIZE, true)
local err_buf = get_string_buf(ERR_BUF_SIZE, true)
local size_ptr = get_size_ptr()
local err_size_ptr = ffi_new("size_t[1]")

local psub
local ngx_lua_ffi_pubsub_sub_new
local ngx_lua_ffi_pubsub_sub_gc
local ngx_lua_ffi_pubsub_subscribe
local ngx_lua_ffi_pubsub_unsubscribe
local ngx_lua_ffi_pubsub_send
local ngx_lua_ffi_pubsub_recv
local ngx_lua_ffi_pubsub_msg_count


if subsystem == "http" then
    ffi.cdef[[
        struct ngx_http_lua_pubsub_sub_s;
        typedef struct ngx_http_lua_pubsub_sub_s ngx_http_lua_pubsub_sub_t;

        int ngx_http_lua_ffi_pubsub_sub_new(ngx_http_lua_pubsub_sub_t **psub,
            int backlog, int clear, int pop_back, char **errmsg);

        void ngx_http_lua_ffi_pubsub_sub_gc(ngx_http_lua_pubsub_sub_t *sub);

        int ngx_http_lua_ffi_pubsub_subscribe(ngx_http_lua_pubsub_sub_t *sub,
            const unsigned char *chan, size_t len, char **errmsg);

        int ngx_http_lua_ffi_pubsub_unsubscribe(ngx_http_lua_pubsub_sub_t *sub,
            char **errmsg);

        int ngx_http_lua_ffi_pubsub_send(const unsigned char *chan, size_t len,
            const unsigned char *data, size_t size, char **errmsg);

        int ngx_http_lua_ffi_pubsub_recv(ngx_http_request_t *r,
            ngx_http_lua_pubsub_sub_t *sub, int wait_ms,
            unsigned char *buf, size_t *buflen,
            unsigned char *err, size_t *errlen);

        int ngx_http_lua_ffi_pubsub_msg_count(ngx_http_lua_pubsub_sub_t *sub);
    ]]

    psub = ffi_new("ngx_http_lua_pubsub_sub_t *[1]")
    ngx_lua_ffi_pubsub_sub_new = C.ngx_http_lua_ffi_pubsub_sub_new
    ngx_lua_ffi_pubsub_sub_gc = C.ngx_http_lua_ffi_pubsub_sub_gc
    ngx_lua_ffi_pubsub_subscribe = C.ngx_http_lua_ffi_pubsub_subscribe
    ngx_lua_ffi_pubsub_unsubscribe = C.ngx_http_lua_ffi_pubsub_unsubscribe
    ngx_lua_ffi_pubsub_send = C.ngx_http_lua_ffi_pubsub_send
    ngx_lua_ffi_pubsub_recv = C.ngx_http_lua_ffi_pubsub_recv
    ngx_lua_ffi_pubsub_msg_count = C.ngx_http_lua_ffi_pubsub_msg_count

elseif subsystem == "stream" then
    ffi.cdef[[
        struct ngx_stream_lua_pubsub_sub_s;
        typedef struct ngx_stream_lua_pubsub_sub_s ngx_stream_lua_pubsub_sub_t;

        int ngx_stream_lua_ffi_pubsub_sub_new(ngx_stream_lua_pubsub_sub_t **psub,
            int backlog, int clear, int pop_back, char **errmsg);

        void ngx_stream_lua_ffi_pubsub_sub_gc(ngx_stream_lua_pubsub_sub_t *sub);

        int ngx_stream_lua_ffi_pubsub_subscribe(ngx_stream_lua_pubsub_sub_t *sub,
            const unsigned char *chan, size_t len, char **errmsg);

        int ngx_stream_lua_ffi_pubsub_unsubscribe(ngx_stream_lua_pubsub_sub_t *sub,
            char **errmsg);

        int ngx_stream_lua_ffi_pubsub_send(const unsigned char *chan, size_t len,
            const unsigned char *data, size_t size, char **errmsg);

        int ngx_stream_lua_ffi_pubsub_recv(ngx_stream_lua_request_t *r,
            ngx_stream_lua_pubsub_sub_t *sub, int wait_ms,
            unsigned char *buf, size_t *buflen,
            unsigned char *err, size_t *errlen);

        int ngx_stream_lua_ffi_pubsub_msg_count(ngx_stream_lua_pubsub_sub_t *sub);
    ]]

    psub = ffi_new("ngx_stream_lua_pubsub_sub_t *[1]")
    ngx_lua_ffi_pubsub_sub_new = C.ngx_stream_lua_ffi_pubsub_sub_new
    ngx_lua_ffi_pubsub_sub_gc = C.ngx_stream_lua_ffi_pubsub_sub_gc
    ngx_lua_ffi_pubsub_subscribe = C.ngx_stream_lua_ffi_pubsub_subscribe
    ngx_lua_ffi_pubsub_unsubscribe = C.ngx_stream_lua_ffi_pubsub_unsubscribe
    ngx_lua_ffi_pubsub_send = C.ngx_stream_lua_ffi_pubsub_send
    ngx_lua_ffi_pubsub_recv = C.ngx_stream_lua_ffi_pubsub_recv
    ngx_lua_ffi_pubsub_msg_count = C.ngx_stream_lua_ffi_pubsub_msg_count
end


local _M = { version = base.version }
local mt = { __index = _M }


function _M.new(backlog, clear, pop_back)
    backlog = tonumber(backlog) or 1
    if backlog < 0 then
        return error("no negative backlog")
    end

    clear = clear and 1 or 0
    pop_back = pop_back and 1 or 0

    local rc = ngx_lua_ffi_pubsub_sub_new(psub, backlog, clear, pop_back,
                                          errmsg)

    if rc == FFI_ERROR then
        return nil, ffi_str(errmsg[0])
    end

    local sub = psub[0]

    ffi_gc(sub, ngx_lua_ffi_pubsub_sub_gc)

    return setmetatable({ sub = sub }, mt)
end


function _M.subscribe(self, chan)
    if type(self) ~= "table" or type(self.sub) ~= "cdata" then
        return error("not a subscriber instance")
    end

    if type(chan) ~= "string" or #chan == 0 then
        return error("bad channel")
    end

    local cdata_sub = self.sub

    local rc = ngx_lua_ffi_pubsub_subscribe(cdata_sub, chan, #chan, errmsg)

    if rc == FFI_ERROR then
        return nil, ffi_str(errmsg[0])
    end

    return true
end


function _M.unsubscribe(self)
    if type(self) ~= "table" or type(self.sub) ~= "cdata" then
        return error("not a subscriber instance")
    end

    local cdata_sub = self.sub

    local rc = ngx_lua_ffi_pubsub_unsubscribe(cdata_sub, errmsg)

    if rc == FFI_ERROR then
        return nil, ffi_str(errmsg[0])
    end

    return true
end


function _M.recv(self, seconds)
    if type(self) ~= "table" or type(self.sub) ~= "cdata" then
        return error("not a subscriber instance")
    end

    local r = get_request()
    if not r then
        return error("no request found")
    end

    local ms = tonumber(seconds) * 1000
    if ms < 0 then
        return error("no negative seconds")
    end

    local cdata_sub = self.sub

    size_ptr[0] = MSG_BUF_SIZE
    err_size_ptr[0] = ERR_BUF_SIZE

    local rc = ngx_lua_ffi_pubsub_recv(r, cdata_sub, ms, msg_buf, size_ptr,
                                       err_buf, err_size_ptr)

    if rc == FFI_ERROR then
        return nil, ffi_str(err_buf, err_size_ptr[0])
    end

    if rc == FFI_OK then
        return ffi_str(msg_buf, size_ptr[0])
    end

    if rc == FFI_DECLINED then
        return nil, "timeout"
    end

    -- Note: we cannot use the tail-call form here since we
    -- might need the current function call's activation
    -- record to hold the reference to our object
    -- to prevent it from getting GC'd prematurely.
    local ok, err = co_yield()
    return ok, err
end


function _M.count(self)
    if type(self) ~= "table" or type(self.sub) ~= "cdata" then
        return error("not a subscriber instance")
    end

    return ngx_lua_ffi_pubsub_msg_count(self.sub)
end


function _M.send(chan, msg)
    if type(chan) ~= "string" or #chan == 0 then
        return error("bad channel")
    end

    if type(msg) ~= "string" then
        return error("bad message")
    end

    local rc = ngx_lua_ffi_pubsub_send(chan, #chan, msg, #msg, errmsg)

    if rc == FFI_ERROR then
        local err = ffi_str(errmsg[0])
        if err == "busy" then
            ngx_sleep(0)
        end

        return nil, err
    end

    return true
end


return _M
