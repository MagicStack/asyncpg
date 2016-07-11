cdef extern from "record/recordobj.h":

	int ApgRecord_CheckExact(object)
	object ApgRecord_New(object, int)
	void ApgRecord_SET_ITEM(object, int, object)
