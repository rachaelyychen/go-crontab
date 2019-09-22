package errno

var (
	// Common errors
	OK                   = &Errno{Code: 0, Message: "OK"}
	InternalServerError  = &Errno{Code: 10001, Message: "Internal server error"}
	ErrBind              = &Errno{Code: 10002, Message: "Error occurred while binding the request body to the struct."}
	ErrDatabase          = &Errno{Code: 20001, Message: "Database error."}
	ErrOperationNotFound = &Errno{Code: 20002, Message: "Operation record not found."}
	ErrAuditNotFound     = &Errno{Code: 20003, Message: "Audit record not found."}
	ErrRecordNotFound    = &Errno{Code: 20004, Message: "Record not found."}
	ErrKeyDuplicate      = &Errno{Code: 20005, Message: "Key duplicate."}
	ErrJsonFormat        = &Errno{Code: 20006, Message: "Json format wrong."}
	ErrConditionType     = &Errno{Code: 20007, Message: "Request condition type wrong."}
	ErrConditionEmpty    = &Errno{Code: 20007, Message: "Request condition is empty."}
	ErrInput             = &Errno{Code: 20202, Message: "Input Error."}
	ErrConvert           = &Errno{Code: 20203, Message: "Convert data Error."}

	ErrInsert = &Errno{Code: 20301, Message: "Insert Error."}
	ErrUpdate = &Errno{Code: 20302, Message: "Update Error."}
	ErrDelete = &Errno{Code: 20303, Message: "Delete Error."}
	ErrGet    = &Errno{Code: 20304, Message: "Get Error."}
	ErrKill   = &Errno{Code: 20305, Message: "Kill Error."}
)
