package replication

import (
	"fmt"
	"strings"
	"time"
	"database/sql"
	"github.com/ngaut/log"
)

var (
	fracTimeFormat []string
)

// fracTime is a help structure wrapping Golang Time.
type fracTime struct {
	time.Time
	tz *time.Location

	// Dec must in [0, 6]
	Dec int
}

func (t fracTime) String() string {
	return t.Format(fracTimeFormat[t.Dec])
}

func formatZeroTime(frac int, dec int, tz *time.Location) string {
	if dec == 0 {
		return "0000-00-00 00:00:00"
	}

	s := fmt.Sprintf("0000-00-00 00:00:00.%06d", frac)

	// dec must < 6, if frac is 924000, but dec is 3, we must output 924 here.
	return s[0 : len(s)-(6-dec)]
}

func init() {
	fracTimeFormat = make([]string, 7)
	fracTimeFormat[0] = "2006-01-02 15:04:05"

	for i := 1; i <= 6; i++ {
		fracTimeFormat[i] = fmt.Sprintf("2006-01-02 15:04:05.%s", strings.Repeat("0", i))
	}
}

// convert time from db.
func TimeFromDB(t *time.Time, tz *time.Location) {
	*t = t.In(tz)
}

// convert time to db.
func TimeToDB(t *time.Time, tz *time.Location) {
	*t = t.In(tz)
}

func detectTZ(ttz *time.Location)  {
	// default use Local
	ttz = time.Local
	var(
		db *sql.DB
		tz string
	)

	row := db.QueryRow("SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP)")
	row.Scan(&tz)
	if len(tz) >= 8 {
		if tz[0] != '-' {
			tz = "+" + tz
		}
		t, err := time.Parse("-07:00:00", tz)
		if err == nil {
			ttz = t.Location()
		} else {
			log.Debugf("Detect DB timezone: %s %s\n", tz, err.Error())
		}
	}
}
