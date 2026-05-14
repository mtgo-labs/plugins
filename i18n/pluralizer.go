package i18n

import (
	"math"

	"golang.org/x/text/language"
)

// PluralRule maps a count to a CLDR plural category string (e.g. "one",
// "few", "many", "other").
type PluralRule func(n int) string

// Pluralizer selects the correct plural variant for a given language and count.
// It ships with built-in rules for common languages and supports custom rules
// via AddRule.
type Pluralizer struct {
	rules map[language.Tag]PluralRule
}

// NewPluralizer creates a Pluralizer pre-loaded with rules for common languages.
func NewPluralizer() *Pluralizer {
	p := &Pluralizer{rules: make(map[language.Tag]PluralRule)}
	p.addBuiltInRules()
	return p
}

// GetVariant returns the plural category for count in the given language.
func (p *Pluralizer) GetVariant(lang language.Tag, count int) string {
	if rule, ok := p.rules[lang]; ok {
		return rule(count)
	}
	base, _ := lang.Base()
	baseTag := language.Make(base.String())
	if rule, ok := p.rules[baseTag]; ok {
		return rule(count)
	}
	if count == 1 {
		return "one"
	}
	return "other"
}

// AddRule registers a custom plural rule for the given language tag.
func (p *Pluralizer) AddRule(lang language.Tag, rule PluralRule) {
	p.rules[lang] = rule
}

func (p *Pluralizer) addBuiltInRules() {
	en := func(n int) string {
		if n == 1 {
			return "one"
		}
		return "other"
	}
	p.rules[language.English] = en
	p.rules[language.German] = en
	p.rules[language.Spanish] = en
	p.rules[language.Italian] = en
	p.rules[language.Portuguese] = en
	p.rules[language.Dutch] = en
	p.rules[language.Swedish] = en
	p.rules[language.Danish] = en
	p.rules[language.Norwegian] = en
	p.rules[language.Finnish] = en
	p.rules[language.Greek] = en
	p.rules[language.Hungarian] = en
	p.rules[language.Turkish] = en
	p.rules[language.Bulgarian] = en

	p.rules[language.French] = func(n int) string {
		if n == 0 || n == 1 {
			return "one"
		}
		return "other"
	}
	p.rules[language.Hindi] = func(n int) string {
		if n == 0 || n == 1 {
			return "one"
		}
		return "other"
	}

	slavic := func(n int) string {
		nAbs := int(math.Abs(float64(n)))
		mod10 := nAbs % 10
		mod100 := nAbs % 100
		if mod10 == 1 && mod100 != 11 {
			return "one"
		}
		if mod10 >= 2 && mod10 <= 4 && !(mod100 >= 12 && mod100 <= 14) {
			return "few"
		}
		if mod10 == 0 || (mod10 >= 5 && mod10 <= 9) || (mod100 >= 11 && mod100 <= 14) {
			return "many"
		}
		return "other"
	}
	p.rules[language.Russian] = slavic
	p.rules[language.Ukrainian] = slavic
	p.rules[language.Serbian] = slavic
	p.rules[language.Croatian] = slavic

	p.rules[language.Polish] = func(n int) string {
		nAbs := int(math.Abs(float64(n)))
		if nAbs == 1 {
			return "one"
		}
		mod10 := nAbs % 10
		mod100 := nAbs % 100
		if (mod10 >= 2 && mod10 <= 4) && !(mod100 >= 12 && mod100 <= 14) {
			return "few"
		}
		if (mod10 != 1 && nAbs != 0) && (mod10 <= 1 || mod10 >= 5) {
			return "many"
		}
		return "other"
	}

	p.rules[language.Arabic] = func(n int) string {
		nAbs := int(math.Abs(float64(n)))
		mod100 := nAbs % 100
		if nAbs == 0 {
			return "zero"
		}
		if nAbs == 1 {
			return "one"
		}
		if nAbs == 2 {
			return "two"
		}
		if mod100 >= 3 && mod100 <= 10 {
			return "few"
		}
		if mod100 >= 11 && mod100 <= 99 {
			return "many"
		}
		return "other"
	}

	p.rules[language.Czech] = func(n int) string {
		if n == 1 {
			return "one"
		}
		if n >= 2 && n <= 4 {
			return "few"
		}
		return "other"
	}
	p.rules[language.Slovak] = p.rules[language.Czech]

	p.rules[language.Hebrew] = func(n int) string {
		if n == 1 {
			return "one"
		}
		if n == 2 {
			return "two"
		}
		if n != 0 && n%10 == 0 {
			return "many"
		}
		return "other"
	}

	p.rules[language.Romanian] = func(n int) string {
		nAbs := int(math.Abs(float64(n)))
		if nAbs == 1 {
			return "one"
		}
		if nAbs == 0 || (nAbs != 1 && nAbs%100 >= 1 && nAbs%100 <= 19) {
			return "few"
		}
		return "other"
	}

	noPlural := func(n int) string { return "other" }
	p.rules[language.Japanese] = noPlural
	p.rules[language.Chinese] = noPlural
	p.rules[language.Korean] = noPlural
	p.rules[language.Vietnamese] = noPlural
	p.rules[language.Thai] = noPlural
	p.rules[language.Indonesian] = noPlural
}
