# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef extern from "record/recordobj.h":

	int ApgRecord_InitTypes() except -1

	int ApgRecord_CheckExact(object)
	object ApgRecord_New(object, int)
	void ApgRecord_SET_ITEM(object, int, object)

	object ApgRecordDesc_New(object, object)
