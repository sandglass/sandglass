package cmdcommon

import (
	"fmt"
)

func DisplayVersion(app, version, commit, date string) string {
	if len(commit) > 7 {
		commit = commit[0:7]
	}
	return fmt.Sprintf("%v %v(%v) - %v", app, version, commit, date)
}
