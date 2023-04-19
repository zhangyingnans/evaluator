package evaluator

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nullne/evaluator/function"
)

func ExampleExpression() {
	exp, err := New(`(eq gender 'male')`)
	if err != nil {
		log.Fatal(err)
	}
	params := MapParams{"gender": "male"}
	res, err := exp.Eval(params)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)
	res, err = exp.EvalBool(params)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)
	// Output:
	// true
	// true
}

func TestBasic(t *testing.T) {
	params := MapParams{
		"gender": "female",
	}
	res, err := EvalBool(`(in gender ("female" "male"))`, params)
	if err != nil {
		t.Error(err)
	}
	if res != true {
		t.Errorf("incorrect result")
	}
	bres, err := Eval(`(in gender ("female" "male"))`, params)
	if err != nil {
		t.Error(err)
	}
	if bres != true {
		t.Errorf("incorrect result")
	}
}

func TestBasicIncorrect(t *testing.T) {
	params := MapParams{
		"gender": "female",
	}
	expr, err := New(`(+ 1 1)`)
	if err != nil {
		t.Error(err)
	}
	_, err = expr.EvalBool(params)
	if err == nil {
		t.Error("should have errors")
	}
}

func TestComplicated(t *testing.T) {
	appVersion, err := function.TypeVersion{}.Eval("2.7.1")
	if err != nil {
		t.Fatal(err)
	}
	params := MapParams{
		"gender":      "female",
		"age":         55,
		"app_version": appVersion,
		"region":      []int{1, 2, 3},
	}
	expr, err := New(`
(or
	(and
	(between age 18 80)
	(eq gender "male")
	(between app_version (t_version "2.7.1") (t_version "2.9.1"))
	)
	(overlap region (2890 3780))
 )`)
	if err != nil {
		t.Error(err)
	}
	_, err = expr.EvalBool(params)
	if err != nil {
		t.Error(err)
	}
}

func TestCorrectBooleanFuncs(t *testing.T) {
	// stmt := `(in gender ("female" "male"))`
	type input struct {
		expr string
		res  interface{}
	}
	now1 := time.Now().Format("2006-01-02 15:04:05")
	now2 := time.Now().Format("2006-01-02 15:04:05")
	appVersion, _ := function.TypeVersion{}.Eval("6.0.0")
	vvf := MapParams{
		"gender":      "male",
		"age":         18,
		"price":       16.7,
		"now1":        now1,
		"now2":        now2,
		"app_version": appVersion,
		"os":          "android",
		"affiliate":   "oppo",
		"language":    "zh-Hans",
	}
	inputs := []input{
		{`(in gender ("female" "male"))`, true},
		{`(in gender () )`, false},
		{`(not (in gender ("female" "male")))`, false},
		{`(! (in gender ("female" "male")))`, false},
		{`(ge (t_version "2.1.1") (t_version "2.1.1"))`, true},
		{`(gt (t_version "2.1.1") (t_version "2.1.1"))`, false},
		{`(between (t_version "2.1.1") (t_version "2.1.1") (t_version "2.1.1"))`, true},
		{`(between (t_version "2.1.1.9999") (t_version "2.1.1") (t_version "2.1.2"))`, true},
		{`(between (mod age 5) 1 3)`, true},
		{`(between (td_time now1) (td_time now1) (td_time now2))`, true},
		{`(ge (t_version "2.8.1") (t_version "2.9.3"))`, false},
		{`(ge (t_version "2.9.1") (t_version "2.8.3"))`, true},

		// overlap
		{`(overlap (1 2 3) (4 5 6))`, false},
		{`(overlap () ())`, false},
		{`(overlap (1 2 3) (4 3 2))`, true},
		{`(overlap ("1" "2" "3") ("4" "3" "2"))`, true},
		{`(and   (or  (and (eq os "android") (ge app_version (t_version "5.6.3") ))     (and (eq os "ios") (ge app_version (t_version "5.7.0") ))   )   (or (and (eq os "android") (not (in affiliate ("googleplay" "huawei" "vivo" "vivo_64" "huawei_64")))) (ne os "android"))   (eq language "zh-Hans") )`, true},
	}
	for _, input := range inputs {
		e, err := New(input.expr)
		if err != nil {
			t.Error(err)
		}
		r, err := e.EvalBool(vvf)
		if err != nil {
			t.Error(err)
		}
		if r != input.res {
			t.Errorf("expression `` %s wanna: %+v, got: %+v", input.expr, input.res, r)
		}
	}
}

func TestVersionParallel(t *testing.T) {
	var versions1 []string
	for i := 0; i < 2000; i++ {
		major := rand.Intn(10) + 1
		minor := rand.Intn(10)
		patch := rand.Intn(10)
		// 定义版本号的后两位
		build := strconv.Itoa(rand.Intn(100))
		// 拼接版本号字符串
		version := fmt.Sprintf("%d.%d.%d.%s", major, minor, patch, build)
		versions1 = append(versions1, version)
	}
	var versions2 []string
	for i := 0; i < 2000; i++ {
		major := rand.Intn(10) + 1
		minor := rand.Intn(10)
		patch := rand.Intn(10)
		// 定义版本号的后两位
		build := strconv.Itoa(rand.Intn(100))
		// 拼接版本号字符串
		version := fmt.Sprintf("%d.%d.%d.%s", major, minor, patch, build)
		versions2 = append(versions2, version)
	}
	exp1, _ := New(`(ge (t_version app_ver1) (t_version app_ver2))`)
	exp2, _ := New(`(lt (t_version app_ver1) (t_version app_ver2))`)
	ch := make(chan MapParams, 10)
	for i := 0; i < 20; i++ {
		go func() {
			for {
				m, ok := <-ch
				if !ok {
					break
				}
				r1, _ := exp1.EvalBool(m)
				r2, _ := exp2.EvalBool(m)
				if (r1 && r2) || (!r1 && !r2) {
					t.Errorf("version check error:%+v", m)
				}
				v1, _ := m.Get("app_ver1")
				v2, _ := m.Get("app_ver2")
				if r1 {
					fmt.Printf("%+v>=%+v\n", v1, v2)
				}
				if r2 {
					fmt.Printf("%+v<%+v\n", v1, v2)
				}
			}
		}()
	}
	for i := 0; i < 2000; i++ {
		vvf := MapParams{
			"app_ver1": versions1[i],
			"app_ver2": versions2[i],
		}
		ch <- vvf
	}
	close(ch)
}

func TestCorrectFuncs(t *testing.T) {
	type input struct {
		expr string
		res  interface{}
	}
	vvf := MapParams{
		"gender": "male",
		"age":    18,
		"price":  16.7,
	}
	inputs := []input{
		{`(eq (mod age 5) 3.0)`, true},
		{`(eq (+ 10 5) 15)`, true},
		{`(eq (/ 10 5) 2)`, true},
	}
	for _, input := range inputs {
		e, err := New(input.expr)
		if err != nil {
			t.Error(err)
		}
		r, err := e.Eval(vvf)
		if err != nil {
			t.Error(err)
		}
		if r != input.res {
			t.Errorf("expression `%s` wanna: %+v, got: %+v", input.expr, input.res, r)
		}

		r, err = Eval(input.expr, vvf)
		if err != nil {
			t.Error(err)
		}
		if r != input.res {
			t.Errorf("expression `%s` wanna: %+v, got: %+v", input.expr, input.res, r)
		}
	}
}

func TestIncorrect(t *testing.T) {
	type input struct {
		expr string
		// expression correctness
		b bool
	}
	vvf := MapParams{
		"gender": "male",
		"age":    18,
		"price":  16.7,
	}
	inputs := []input{
		{`eq (mod age 5) 3.0`, false},
		{`(eq (+ money 5) 15)`, true},
	}
	for _, input := range inputs {
		_, err := New(input.expr)
		if !input.b && err == nil {
			t.Error("should have errors")
		}

		_, err = Eval(input.expr, vvf)
		if err == nil {
			t.Errorf("input: %v, should have errors", input.expr)
		}

		_, err = EvalBool(input.expr, vvf)
		if err == nil {
			t.Errorf("input: %v, should have errors", input.expr)
		}
	}
}

func TestAdvancedFunc(t *testing.T) {
	invoker := func(params ...interface{}) (interface{}, error) {
		fn := params[0].(function.Func)
		return fn(params[1:]...)
	}

	if err := function.Regist("invoke", invoker); err != nil {
		t.Error(err)
	}

	exp := `(invoke + 1 1)`
	e, err := New(exp)
	if err != nil {
		t.Error(err)
	}
	r, err := e.Eval(nil)
	if err != nil {
		t.Error(err)
	}
	if r.(float64) != 2 {
		t.Errorf("expression `%s` wanna: %+v, got: %+v", exp, 2, r)
	}
}

func TestDIVFunc(t *testing.T) {
	age := func(params ...interface{}) (interface{}, error) {
		if len(params) != 1 {
			return nil, errors.New("only one params accepted")
		}
		birth, ok := params[0].(string)
		if !ok {
			return nil, errors.New("birth format need to be string")
		}
		r, err := time.Parse("2006-01-02", birth)
		if err != nil {
			return nil, err
		}
		now := time.Now()
		a := r.Year() - now.Year()
		if r.Month() < now.Month() {
			a--
		} else if r.Month() == now.Month() {
			if r.Day() < now.Day() {
				a--
			}
		}
		return a, nil
	}

	if err := function.Regist("age", age); err != nil {
		t.Error(err)
	}

	exp := `(not (between (age birthdate) 18 20))`
	vvf := MapParams{
		"birthdate": "2018-02-01",
	}
	e, err := New(exp)
	if err != nil {
		t.Error(err)
	}
	r, err := e.Eval(vvf)
	if err != nil {
		t.Error(err)
	}
	if r != true {
		t.Errorf("expression `%s` wanna: %+v, got: %+v", exp, true, r)
	}
}

func TestHashFunc(t *testing.T) {
	hash := func(params ...interface{}) (interface{}, error) {
		if l := len(params); l != 2 {
			return false, fmt.Errorf("between: need only two param, but got %d", l)
		}

		input := fmt.Sprintf("%v%v", params[0], params[1])
		has := md5.Sum([]byte(input))
		encodedStr := fmt.Sprintf("%x", has)[0:6]
		bi := big.NewInt(0)
		bi.SetString(encodedStr, 16)
		return bi.Uint64(), nil
	}
	if err := function.Regist("hash_with_exp2", hash); err != nil {
		t.Error(err)
	}
	uids := getUID()
	exp := `(in (mod (hash_with_exp2 user_id 109947) 300) (0 4 6 7 8 9 11 13 15 16 19 20 21 22 23 24 25 32 35 36 38 39 40 43 48 50 52 57 59 60 63 64 65 68 70 71 74 75 76 77 81 85 86 91 92 96 98 99 101 103 108 111 113 119 121 124 125 126 127 129 131 132 133 134 139 143 144 147 150 152 153 154 155 156 158 160 162 163 165 169 170 174 176 177 178 180 181 182 184 185 186 189 191 194 197 199 201 206 207 208 209 211 212 214 219 220 224 225 227 228 229 230 232 233 236 237 239 241 242 243 244 246 247 250 253 254 255 260 263 269 270 271 273 275 276 278 279 281 282 283 289 290 291 293 294 295 296 297 298 299))`
	e1, err := New(exp)
	if err != nil {
		t.Error(err)
	}
	exp = `(in (mod (hash_with_exp2 user_id 109947) 300) (1 2 3 5 10 12 14 17 18 26 27 28 29 30 31 33 34 37 41 42 44 45 46 47 49 51 53 54 55 56 58 61 62 66 67 69 72 73 78 79 80 82 83 84 87 88 89 90 93 94 95 97 100 102 104 105 106 107 109 110 112 114 115 116 117 118 120 122 123 128 130 135 136 137 138 140 141 142 145 146 148 149 151 157 159 161 164 166 167 168 171 172 173 175 179 183 187 188 190 192 193 195 196 198 200 202 203 204 205 210 213 215 216 217 218 221 222 223 226 231 234 235 238 240 245 248 249 251 252 256 257 258 259 261 262 264 265 266 267 268 272 274 277 280 284 285 286 287 288 292))`
	e2, err := New(exp)
	if err != nil {
		t.Error(err)
	}
	exp = `(and  (overlap region region) (not (or (eq user_source "facebook") (and (eq user_source "mobile") (ne country_code 86)) (or (not (overlap register_region (3142))) (overlap register_region (3130 3131)))))   (eq gender "male")   (not (overlap region (3487)))   (not (ge live_rich_user 25))   (in (mod (hash_with_exp2 user_id 541) 1000) (0 2 3 7 14 17 26 29 30 33 35 36 39 40 48 49 50 52 57 59 64 67 73 74 76 78 82 84 86 89 98 103 107 112 114 115 117 121 123 124 126 133 139 144 147 148 149 154 158 160 169 172 174 176 183 185 186 195 197 198 199 209 210 212 213 215 216 224 226 228 230 235 236 242 247 248 249 250 251 254 264 267 269 272 274 275 276 278 280 282 284 287 289 294 295 297 298 299 306 307 308 313 316 317 330 336 337 342 356 357 361 363 364 367 372 374 375 377 387 388 391 394 397 399 409 411 412 415 420 432 433 444 447 450 456 464 465 468 481 485 488 491 492 493 499 501 505 515 516 518 526 527 530 533 536 538 539 547 549 551 553 558 560 566 571 572 573 575 578 579 580 581 582 583 584 586 587 590 593 595 598 601 603 607 608 611 614 615 616 618 619 622 625 637 639 640 641 645 647 648 650 652 654 655 657 660 661 665 671 674 685 687 690 694 705 706 707 708 711 712 718 722 723 726 733 735 737 738 740 748 755 759 760 763 764 765 769 771 773 783 786 787 790 791 793 796 808 809 814 816 818 821 827 831 837 840 842 843 845 847 848 850 851 855 856 857 860 868 870 875 877 878 882 883 889 899 901 906 907 908 912 914 921 923 927 928 929 931 933 934 939 950 954 955 958 961 963 965 967 968 971 974 975 978 979 983 992 995 998 999)) )`
	e3, err := New(exp)
	if err != nil {
		t.Error(err)
	}
	ch := make(chan MapParams)
	gCnt := 20
	if len(uids) < gCnt {
		gCnt = len(uids)
	}
	var n int64
	for i := 0; i < gCnt; i++ {
		go func() {
			for {
				m, ok := <-ch
				if !ok {
					break
				}
				r1, _ := e1.EvalBool(m)
				r2, _ := e2.EvalBool(m)
				if (r1 && r2) || (!r1 && !r2) {
					t.Errorf("hash_with_exp2 check error:%+v", m)
				}
				r3, err := e3.EvalBool(m)
				if err != nil {
					t.Errorf("e3 error:%+v", err)
					break
				}
				if r3 {
					atomic.AddInt64(&n, 1)
				}
			}
		}()
	}
	for _, uid := range uids {
		vvf := MapParams{
			"user_id":         uid,
			"user_source":     "qq",
			"country_code":    86,
			"register_region": []int64{3142, 100, 200, 900, 1000, 1200},
			"gender":          "male",
			"live_rich_user":  20,
			"region":          []int64{3142, 100, 200, 900, 1000, 1200},
		}
		ch <- vvf
	}
	time.Sleep(time.Second)
	close(ch)
	fmt.Printf("match count:%+v\n", n)
}

func getUID() []int64 {
	b, e := ioutil.ReadFile("/Users/zhangyingnan/Desktop/user_id_demo")
	if e != nil {
		return []int64{123423163, 124074845, 124722412, 125058598, 125148570, 125466261, 125866931, 125957417, 126432913}
	}
	uidStrs := strings.Split(string(b), "\n")
	var uids []int64
	for _, s := range uidStrs {
		uid, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err == nil {
			uids = append(uids, uid)
		}
	}
	return uids
}

func TestProperties(t *testing.T) {
	type input struct {
		expr string
		res  []string
	}
	inputs := []input{
		{
			expr: `(or
	(and
	(between AGE 18 80)
	(eq gender "male")
	(between app_version (t_version "2.7.1") (t_version "2.9.1"))
	)
	(overlap region (2890 3780))
	 )`,
			res: []string{"AGE", "gender", "app_version", "region"},
		},
		{
			expr: `(and
  (and (eq os "android") (ge app_version (t_version "4.0.6") ))
  (or (and (eq os "android") (ne affiliate "googleplay")) (ne os "android"))
  (eq language "zh-Hans")
	)`,
			res: []string{"os", "app_version", "os", "affiliate", "os", "language"},
		},
		{
			expr: `(eq 1 1)`,
			res:  nil,
		},
		{
			expr: `(and () ())`,
			res:  nil,
		},
	}
	for _, input := range inputs {
		e, err := New(input.expr)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(e.Properties(), input.res) {
			t.Errorf("not equal res: %+v, want: %+v", e.Properties(), input.res)
		}
	}
}

func BenchmarkEqualString(b *testing.B) {
	expr, err := New(`(eq "one" "one" "three")`)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		_, err := expr.EvalBool(nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkInString(b *testing.B) {
	expr, err := New(`(in "2.7.2" ("2.7.1" "2.7.4" "2.7.5"))`)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		_, err := expr.EvalBool(nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkBetweenInt(b *testing.B) {
	expr, err := New(`(between 100 10 200)`)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		_, err := expr.EvalBool(nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkBetweenTime(b *testing.B) {
	expr, err := New(`(between (td_time "2017-07-09 12:00:00") (td_time "2017-07-02 12:00:00") (td_time "2017-07-19 12:00:00"))`)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		_, err := expr.EvalBool(nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkOverlapInt(b *testing.B) {
	expr, err := New(`(overlap (1 2 3) (4 5 6))`)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		_, err := expr.EvalBool(nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkTypeTime(b *testing.B) {
	expr, err := New(`(td_time "2017-09-09 12:00:00")`)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		_, err := expr.Eval(nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkTypeVersion(b *testing.B) {
	expr, err := New(`(t_version "2.8.9.1")`)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		_, err := expr.Eval(nil)
		if err != nil {
			b.Error(err)
		}
	}
}
