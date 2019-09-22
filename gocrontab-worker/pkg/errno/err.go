package errno

import "errors"

/**
* @project: gocrontab-worker
*
* @description:
*
* @author: cyy
*
* @create: 9/13/19 2:01 PM
**/

var (
	LockBusyErr = errors.New("Lock already in use.")
	RecordNotFoundErr = errors.New("Record not found.")
)
