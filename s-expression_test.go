// thanks for the insight from https://rosettacode.org/wiki/S-Expressions#Go

package evaluator

import "testing"

func TestFscan(t *testing.T) {
	type input struct {
		exp string
		err error
	}
	inputs := []input{
		{` `, nil},
		{`"\\\\"`, nil},
		{`a`, nil},
		{`(a)`, nil},
		{`(a b c)`, nil},
		{`(!(a b c))`, nil},
		{`(not (a b c))`, nil},
		{`(a b ( c d ) )`, nil},
		{`(a b ( c d () ) )`, nil},
		{`(a '(' ')' b ( c ) (d e (f g)))`, nil},
		{`((data "quoted data" 123 4.5) (data (!@# (4.5) "(more" "data)")))`, nil},
		{`((data "quoted data" ")" 123 4.5) (data (!@# (4.5) "(more" "data)")))`, nil},
		{``, ErrNilInput},
		{`()`, ErrNilInput},
		{`("a" 'b)`, ErrUnexpectedEnd},
		{`(a b) c`, ErrLeftOverText},
		{`(a b) ( c d )`, ErrLeftOverText},
		{`'(' "(" "\"(\"" a b c)`, ErrUnmatchedParenthesis},
		{`(and (and (in var1 ( 12  20  23 )) (in var2 ( 12  20  23 )) (and (not (overlap var3 ( 4  5  6 ))) (overlap ( 4  5  6 ) ( 7  8  9 )))) (overlap ( 1  2  3 ) ( 4  5  6 )))`, nil},
	}
	for _, input := range inputs {
		s, err := parse(input.exp)
		optimization(s)
		if err != input.err {
			t.Errorf("wanna: %v, got: %v", input.err, err)
		}
	}
}

func ExampleExpression_ignore() {
	s := `(!(a b )())`
	exp, err := parse(s)
	if err != nil {
		return
	}
	exp.dump(0)
	// Output:
	// evaluator.list: 3 elements: [! [a b] []]
	//    evaluator.varString: !
	//    evaluator.list: 2 elements: [a b]
	//       evaluator.varString: a
	//       evaluator.varString: b
	//    evaluator.list: 0 elements: []
}

func TestFscanStringWithQuotesStriped(t *testing.T) {
	type input struct {
		data    string
		advance int
		token   string
		err     error
	}
	data := []input{
		{``, 0, ``, nil},
		{`strings`, 7, `tring`, nil},
		{`st'rings`, 8, `t'ring`, nil},
		{`"string"`, 8, `string`, nil},
		{`"str'ing"`, 9, `str'ing`, nil},
		{`"str ing"`, 9, `str ing`, nil},
		{`'string'`, 8, `string`, nil},
		{`'str\'ing'`, 10, `str'ing`, nil},
		{`'str\\'ing'`, 7, `str\\`, nil},
		{`'str\\\'ing'`, 12, `str\\'ing`, nil},
		{`'`, 0, ``, ErrUnexpectedEnd},
		{`string`, 0, ``, ErrUnexpectedEnd},
		{`'str\\\'`, 0, ``, ErrUnexpectedEnd},
		{`st'ring`, 0, ``, ErrUnexpectedEnd},
	}
	for _, d := range data {
		advance, token, err := scanStringWithQuotesStriped([]byte(d.data))
		if d.advance != advance ||
			d.token != string(token) ||
			d.err != err {
			t.Errorf("%s, advanced, token, error expected to be  (%d, %s, %v),  but (%d, %s, %v)", d.data, d.advance, d.token, d.err, advance, token, err)
		}
	}
}
