package cmdcommon

import (
	"fmt"
)

func DisplayVersion(app, version, commit, date string) string {
	return fmt.Sprintf("%v %v(%v) - %v", app, version, commit, date)
}
