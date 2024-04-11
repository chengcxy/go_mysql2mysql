package errors

type Error interface {
	Code() int
	Error() string
}

type theError struct {
	code int
	msg  string
}

func NewError(code int, errMsg string) Error {
	return theError{
		code: code,
		msg:  errMsg,
	}
}

func (p theError) Code() int {
	return p.code
}
func (p theError) Error() string {
	return p.msg
}

var (
	ParamsError Error = theError{400, "params error"}
	ServerError Error = theError{500, "Server error"}
)
