package backend

import "github.com/versity/versitygw/s3err"

const (
	defaultInvalidArgumentDescription = "Invalid request argument."
	copySourceArgumentName            = "x-amz-copy-source"
)

type requiredS3Argument struct {
	name    string
	missing bool
}

func requiredArg(name string, missing bool) requiredS3Argument {
	return requiredS3Argument{name: name, missing: missing}
}

func invalidArgument(name string) s3err.InvalidArgumentError {
	return s3err.InvalidArgumentError{
		Description:  defaultInvalidArgumentDescription,
		ArgumentName: name,
	}
}

func missingRequiredArgument(args ...requiredS3Argument) s3err.InvalidArgumentError {
	for _, arg := range args {
		if arg.missing {
			return invalidArgument(arg.name)
		}
	}
	return invalidArgument("")
}
