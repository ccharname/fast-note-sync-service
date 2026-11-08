package service

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMerge3(t *testing.T) {
	cases := []struct {
		name         string
		base, s, c   string
		want         string
		wantOK       bool
	}{
		{"identical", "a\n", "a\n", "a\n", "a\n", true},
		{"only client changed", "a\nb\n", "a\nb\n", "a\nX\n", "a\nX\n", true},
		{"only server changed", "a\nb\n", "a\nY\n", "a\nb\n", "a\nY\n", true},
		{"disjoint edits", "1\n2\n3\n4\n5\n", "1\nS\n3\n4\n5\n", "1\n2\n3\nC\n5\n", "1\nS\n3\nC\n5\n", true},
		{"both same edit", "1\n2\n3\n", "1\nX\n3\n", "1\nX\n3\n", "1\nX\n3\n", true},
		{"same-line conflict", "1\n2\n3\n", "1\nS\n3\n", "1\nC\n3\n", "", false},
		{"adjacent edits conflict", "1\n2\n3\n4\n", "1\nS\n3\n4\n", "1\n2\nC\n4\n", "", false},
		{"same-point insert: submitter first", "a\nz\n", "a\nS\nz\n", "a\nC\nz\n", "a\nC\nS\nz\n", true},
		{"same insert both sides dedups", "a\nz\n", "a\nX\nz\n", "a\nX\nz\n", "a\nX\nz\n", true},
		{"empty base both insert", "", "s\n", "c\n", "c\ns\n", true},
		{"empty everything", "", "", "", "", true},
		{"client deletes all, server untouched", "a\nb\n", "a\nb\n", "", "", true},
		{"delete vs edit elsewhere", "1\n2\n3\n4\n5\n", "1\n2\n3\n5\n", "1\nC\n3\n4\n5\n", "1\nC\n3\n5\n", true},
		{"delete vs edit same line conflicts", "1\n2\n3\n", "1\n3\n", "1\nC\n3\n", "", false},
		{"no trailing newline, both append", "a", "a\nS", "a\nC", "a\nC\nS", true},
		{"trailing newline added by client", "a\nb", "a\nb", "a\nb\n", "a\nb\n", true},
		{"trailing newline added by server, client edits", "a\nb\nc", "a\nb\nc\n", "A\nb\nc", "A\nb\nc\n", true},
		{"CRLF preserved", "a\r\nb\r\nc\r\nd\r\n", "A\r\nb\r\nc\r\nd\r\n", "a\r\nb\r\nc\r\nD\r\n", "A\r\nb\r\nc\r\nD\r\n", true},
		{"CRLF vs LF line is a different line", "a\r\nx\r\nb\r\n", "a\nx\r\nb\r\n", "a\r\nx\r\nB\r\n", "a\nx\r\nB\r\n", true},
		{"chinese and emoji", "第一行\n第二行\n第三行\n第四行\n", "第一行 🌊\n第二行\n第三行\n第四行\n", "第一行\n第二行\n第三行\n第四行 🐳\n", "第一行 🌊\n第二行\n第三行\n第四行 🐳\n", true},
		{"blank lines", "a\n\n\nb\n", "a\n\n\nb\nS\n", "C\na\n\n\nb\n", "C\na\n\n\nb\nS\n", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Merge3(tc.base, tc.s, tc.c)
			assert.Equal(t, tc.wantOK, ok)
			if ok {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestSplitJoinLossless(t *testing.T) {
	for _, s := range []string{"", "\n", "\n\n", "a", "a\n", "a\n\nb", "x\r\ny\r\n", "末尾无换行"} {
		l, nl := splitLines(s)
		assert.Equal(t, s, joinLines(l, nl), "%q", s)
	}
}

func mkLines(n int, tag string) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "line %d %s lorem ipsum dolor sit amet consectetur\n", i, tag)
	}
	return b.String()
}

// mkLinesWide: ~115 B/line, so 9000 lines is ~1 MB while three copies stay under diff3MaxLines.
func mkLinesWide(n int, tag string) string {
	var b strings.Builder
	pad := strings.Repeat("lorem ipsum ", 5)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "line %d %s %s\n", i, tag, pad)
	}
	return b.String()
}

func TestMerge3Budget(t *testing.T) {
	big := mkLines(diff3MaxLines+5, "x")
	_, ok := Merge3(big, big+"S\n", big+"C\n")
	assert.False(t, ok, "over line budget -> conflict")

	// edit distance over budget: completely different contents of moderate size
	base := mkLines(3000, "b")
	s := mkLines(3000, "s")
	c := mkLines(3000, "c")
	start := time.Now()
	_, ok = Merge3(base, s, c)
	assert.False(t, ok, "edit distance over budget -> conflict")
	assert.Less(t, time.Since(start), 5*time.Second)
}

// property: merging with random disjoint edits keeps every edit
func TestMerge3RandomDisjoint(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		n := 40
		base := make([]string, n)
		for i := range base {
			base[i] = fmt.Sprintf("l%d", i)
		}
		s := append([]string(nil), base...)
		c := append([]string(nil), base...)
		want := append([]string(nil), base...)
		for i := 1; i < n-1; i += 3 { // every 3rd line: never adjacent
			switch r.Intn(3) {
			case 0:
				s[i] = "S" + s[i]
				want[i] = s[i]
			case 1:
				c[i] = "C" + c[i]
				want[i] = c[i]
			}
		}
		got, ok := Merge3(strings.Join(base, "\n")+"\n", strings.Join(s, "\n")+"\n", strings.Join(c, "\n")+"\n")
		assert.True(t, ok)
		assert.Equal(t, strings.Join(want, "\n")+"\n", got)
	}
}

// 1 MB note, a few scattered edits on each side: must merge well under 200 ms.
func BenchmarkMerge3_1MB(b *testing.B) {
	base := mkLinesWide(9000, "base") // ~1.0 MB
	bl := strings.Split(base, "\n")
	sl := append([]string(nil), bl...)
	cl := append([]string(nil), bl...)
	for i := 100; i < 8500; i += 997 {
		sl[i] = "SERVER " + sl[i]
	}
	for i := 600; i < 8500; i += 1013 {
		cl[i] = "CLIENT " + cl[i]
	}
	s, c := strings.Join(sl, "\n"), strings.Join(cl, "\n")
	b.SetBytes(int64(len(base)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := Merge3(base, s, c); !ok {
			b.Fatal("conflict")
		}
	}
}

func TestMerge3_1MBUnder200ms(t *testing.T) {
	base := mkLinesWide(9000, "base")
	bl := strings.Split(base, "\n")
	sl := append([]string(nil), bl...)
	cl := append([]string(nil), bl...)
	for i := 100; i < 8500; i += 997 {
		sl[i] = "SERVER " + sl[i]
	}
	for i := 600; i < 8500; i += 1013 {
		cl[i] = "CLIENT " + cl[i]
	}
	start := time.Now()
	_, ok := Merge3(base, strings.Join(sl, "\n"), strings.Join(cl, "\n"))
	d := time.Since(start)
	assert.True(t, ok)
	assert.Less(t, d, 200*time.Millisecond, "took %s", d)
}

// dense edits (edit distance ~1400 per side, close to the budget) on a 1 MB note.
func TestMerge3_1MBDenseUnder200ms(t *testing.T) {
	base := mkLinesWide(9000, "base")
	bl := strings.Split(base, "\n")
	sl := append([]string(nil), bl...)
	cl := append([]string(nil), bl...)
	for i := 10; i < 8500; i += 23 {
		sl[i] = "SERVER " + sl[i]
	}
	for i := 15; i < 8500; i += 29 {
		cl[i] = "CLIENT " + cl[i]
	}
	start := time.Now()
	_, ok := Merge3(base, strings.Join(sl, "\n"), strings.Join(cl, "\n"))
	d := time.Since(start)
	t.Logf("dense merge ok=%v took %s", ok, d)
	assert.Less(t, d, 200*time.Millisecond)
}
