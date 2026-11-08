package service

import (
	"slices"
	"strings"
)

// lucky-v2: line-level three-way merge (diff3) used by the server-side merge.
// Lines are interned to ints and diffed with Myers' O(ND) algorithm under a budget;
// anything over budget is reported as a conflict (caller stores a conflict copy).
// lucky-v2：行级三方合并。行先映射为整数再做 Myers diff，带预算；超限按冲突处理。

const (
	diff3MaxBytes = 2 << 20 // any single side above this -> conflict, checked before splitting // 单侧字节上限，split 前判断
	diff3MaxLines = 30000   // total lines of base+server+client // 三方合计行数上限
	diff3MaxEdits = 1000    // max Myers edit distance per diff // 单次 diff 的最大编辑距离
	diff3MaxConc  = 2       // process-wide concurrent merges // 全局合并并发
)

// MergeStatus is why a merge did or did not produce a result.
type MergeStatus int

const (
	MergeOK      MergeStatus = iota
	MergeOverlap             // overlapping edits
	MergeBudget              // bytes / lines / edit distance over budget
)

var mergeSem = make(chan struct{}, diff3MaxConc)

// mergeBudgeted runs Merge3Status under the global concurrency limit.
func mergeBudgeted(base, s, c string) (string, MergeStatus) {
	mergeSem <- struct{}{}
	defer func() { <-mergeSem }()
	return Merge3Status(base, s, c)
}

// splitLines splits text into lines without terminators plus whether the text ends with "\n".
// "\r" is kept as part of the line so CRLF is preserved. Lossless: joinLines(splitLines(s)) == s.
func splitLines(s string) ([]string, bool) {
	if s == "" {
		return nil, false
	}
	parts := strings.Split(s, "\n")
	if parts[len(parts)-1] == "" {
		return parts[:len(parts)-1], true
	}
	return parts, false
}

func joinLines(lines []string, trailingNL bool) string {
	if len(lines) == 0 {
		return ""
	}
	out := strings.Join(lines, "\n")
	if trailingNL {
		out += "\n"
	}
	return out
}

// Merge3 merges client text c and server text s against common base. On a same-point pure
// insertion both sides are kept with the submitter (c) first. ok=false means conflict or
// over budget.
// Merge3 以 base 为基准合并服务端文本 s 与提交方文本 c；同点纯插入两侧都保留，提交方在前。
func Merge3(base, s, c string) (merged string, ok bool) {
	m, st := Merge3Status(base, s, c)
	return m, st == MergeOK
}

// Merge3Status is Merge3 with the failure reason.
func Merge3Status(base, s, c string) (merged string, st MergeStatus) {
	if s == c {
		return s, MergeOK
	}
	if s == base {
		return c, MergeOK
	}
	if c == base {
		return s, MergeOK
	}
	if len(base) > diff3MaxBytes || len(s) > diff3MaxBytes || len(c) > diff3MaxBytes {
		return "", MergeBudget
	}
	bl, bnl := splitLines(base)
	sl, snl := splitLines(s)
	cl, cnl := splitLines(c)
	if len(bl)+len(sl)+len(cl) > diff3MaxLines {
		return "", MergeBudget
	}

	intern := make(map[string]int, len(bl)+len(sl)+len(cl))
	id := func(lines []string) []int {
		out := make([]int, len(lines))
		for i, l := range lines {
			v, ok := intern[l]
			if !ok {
				v = len(intern)
				intern[l] = v
			}
			out[i] = v
		}
		return out
	}
	bi, si, ci := id(bl), id(sl), id(cl)

	ms, ok1 := lcsMatch(bi, si, diff3MaxEdits)
	if !ok1 {
		return "", MergeBudget // first side already over budget: do not diff the second
	}
	mc, ok2 := lcsMatch(bi, ci, diff3MaxEdits)
	if !ok2 {
		return "", MergeBudget
	}

	// trailing newline: three-way scalar merge
	var nl bool
	switch {
	case snl == bnl:
		nl = cnl
	case cnl == bnl:
		nl = snl
	default: // both changed from base; bools can only agree
		nl = snl
	}

	out := make([]string, 0, len(sl)+len(cl))
	pb, ps, pc := 0, 0, 0
	region := func(eb, es, ec int) bool {
		bseg, sseg, cseg := bi[pb:eb], si[ps:es], ci[pc:ec]
		switch {
		case slices.Equal(sseg, bseg):
			out = append(out, cl[pc:ec]...)
		case slices.Equal(cseg, bseg):
			out = append(out, sl[ps:es]...)
		case slices.Equal(sseg, cseg):
			out = append(out, sl[ps:es]...)
		case len(bseg) == 0: // same-point insertions: submitter first
			out = append(out, cl[pc:ec]...)
			out = append(out, sl[ps:es]...)
		default:
			return false
		}
		return true
	}
	for i := range bi {
		if ms[i] < 0 || mc[i] < 0 {
			continue
		}
		if !region(i, ms[i], mc[i]) {
			return "", MergeOverlap
		}
		out = append(out, bl[i])
		pb, ps, pc = i+1, ms[i]+1, mc[i]+1
	}
	if !region(len(bi), len(si), len(ci)) {
		return "", MergeOverlap
	}
	return joinLines(out, nl), MergeOK
}

// lcsMatch returns m where m[i] is the index in b matched with a[i] (or -1), from a Myers
// diff. ok=false when the edit distance exceeds maxD.
func lcsMatch(a, b []int, maxD int) (m []int, ok bool) {
	m = make([]int, len(a))
	for i := range m {
		m[i] = -1
	}
	p := 0
	for p < len(a) && p < len(b) && a[p] == b[p] {
		m[p] = p
		p++
	}
	sfx := 0
	for sfx < len(a)-p && sfx < len(b)-p && a[len(a)-1-sfx] == b[len(b)-1-sfx] {
		m[len(a)-1-sfx] = len(b) - 1 - sfx
		sfx++
	}
	x, y := a[p:len(a)-sfx], b[p:len(b)-sfx]
	if len(x) == 0 || len(y) == 0 {
		return m, true
	}
	pairs, ok := myers(x, y, maxD)
	if !ok {
		return nil, false
	}
	for _, pr := range pairs {
		m[p+pr[0]] = p + pr[1]
	}
	return m, true
}

// myers computes matched index pairs (LCS) between a and b; ok=false if D > maxD.
func myers(a, b []int, maxD int) (pairs [][2]int, ok bool) {
	n, mm := len(a), len(b)
	max := n + mm
	if max > maxD {
		max = maxD
	}
	off := max + 1
	v := make([]int, 2*max+3)
	trace := make([][]int, 0, 16)
	D := -1
loop:
	for d := 0; d <= max; d++ {
		for k := -d; k <= d; k += 2 {
			var xx int
			if k == -d || (k != d && v[off+k-1] < v[off+k+1]) {
				xx = v[off+k+1]
			} else {
				xx = v[off+k-1] + 1
			}
			yy := xx - k
			for xx < n && yy < mm && a[xx] == b[yy] {
				xx++
				yy++
			}
			v[off+k] = xx
			if xx >= n && yy >= mm {
				D = d
				break loop
			}
		}
		snap := make([]int, 2*d+1)
		copy(snap, v[off-d:off+d+1])
		trace = append(trace, snap)
	}
	if D < 0 {
		return nil, false
	}
	x, y := n, mm
	for d := D; d > 0; d-- {
		vp := trace[d-1] // index k+(d-1)
		k := x - y
		var pk int
		if k == -d || (k != d && vp[k-1+d-1] < vp[k+1+d-1]) {
			pk = k + 1
		} else {
			pk = k - 1
		}
		px := vp[pk+d-1]
		py := px - pk
		var mx int
		if pk == k+1 {
			mx = px
		} else {
			mx = px + 1
		}
		my := mx - k
		for x > mx && y > my {
			x--
			y--
			pairs = append(pairs, [2]int{x, y})
		}
		x, y = px, py
	}
	for x > 0 && y > 0 {
		x--
		y--
		pairs = append(pairs, [2]int{x, y})
	}
	return pairs, true
}
