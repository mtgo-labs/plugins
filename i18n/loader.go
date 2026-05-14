package i18n

import (
	"embed"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func (t *Translator) loadEmbeddedLocales(efs embed.FS, dir string, langs []language.Tag) {
	if dir == "" {
		dir = "."
	}
	ext := t.fileExtension()
	if len(langs) == 0 {
		langs = t.discoverLangs(efs, dir, ext)
	}
	for _, lang := range langs {
		langDir := path.Join(dir, lang.String())
		if entries, err := fs.ReadDir(efs, langDir); err == nil {
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), "."+ext) {
					continue
				}
				content, err := efs.ReadFile(path.Join(langDir, entry.Name()))
				if err != nil {
					continue
				}
				t.loadContent(lang, content)
			}
			continue
		}
		filePath := path.Join(dir, lang.String()+"."+ext)
		content, err := efs.ReadFile(filePath)
		if err != nil {
			content, err = efs.ReadFile(lang.String() + "." + ext)
			if err != nil {
				continue
			}
		}
		t.loadContent(lang, content)
	}
}

func (t *Translator) discoverLangs(efs embed.FS, dir, ext string) []language.Tag {
	entries, err := fs.ReadDir(efs, dir)
	if err != nil {
		return nil
	}
	var langs []language.Tag
	seen := make(map[language.Tag]bool)
	for _, entry := range entries {
		name := entry.Name()
		var tagStr string
		switch {
		case entry.IsDir():
			tagStr = name
		case strings.HasSuffix(name, "."+ext):
			tagStr = strings.TrimSuffix(name, "."+ext)
		default:
			continue
		}
		tag, err := language.Parse(tagStr)
		if err != nil {
			continue
		}
		if !seen[tag] {
			seen[tag] = true
			langs = append(langs, tag)
		}
	}
	return langs
}

func (t *Translator) loadContent(lang language.Tag, content []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.format {
	case FormatFTL:
		t.loadFTLLocked(lang, string(content))
	default:
		t.loadYAMLLocked(lang, content)
	}
}

func (t *Translator) fileExtension() string {
	switch t.format {
	case FormatFTL:
		return "ftl"
	default:
		return "yaml"
	}
}

// LoadYAML parses YAML content and merges it into the locale for lang.
func (t *Translator) LoadYAML(lang language.Tag, content []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.loadYAMLLocked(lang, content)
}

func (t *Translator) loadYAMLLocked(lang language.Tag, content []byte) error {
	var data map[string]any
	if err := yaml.Unmarshal(content, &data); err != nil {
		return fmt.Errorf("yaml parse: %w", err)
	}
	locale := t.getOrCreateLocale(lang)
	t.parseYAMLMap(data, "", locale.Messages)
	return nil
}

func (t *Translator) parseYAMLMap(data map[string]any, prefix string, messages map[string]*Message) {
	for key, value := range data {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "." + key
		}
		switch v := value.(type) {
		case map[string]any:
			if isVariantMap(v) {
				msg := &Message{Key: fullKey, Variants: make(map[string]string)}
				for vk, vv := range v {
					sv := fmt.Sprintf("%v", vv)
					if vk == "other" || vk == "base" {
						msg.Value = sv
					} else {
						msg.Variants[vk] = sv
					}
				}
				if msg.Value == "" && len(v) > 0 {
					for _, val := range v {
						msg.Value = fmt.Sprintf("%v", val)
						break
					}
				}
				messages[fullKey] = msg
			} else {
				t.parseYAMLMap(v, fullKey, messages)
			}
		case string:
			messages[fullKey] = &Message{Key: fullKey, Value: v}
		}
	}
}

var knownPluralForms = map[string]bool{
	"zero": true, "one": true, "two": true,
	"few": true, "many": true, "other": true, "base": true,
}

func isVariantMap(m map[string]any) bool {
	if len(m) == 0 {
		return false
	}
	for k, v := range m {
		if !knownPluralForms[k] {
			return false
		}
		if _, ok := v.(string); !ok {
			return false
		}
	}
	return true
}

func (t *Translator) loadFTLLocked(lang language.Tag, content string) {
	parser := NewFTLParser()
	messages, _ := parser.Parse(content)
	locale := t.getOrCreateLocale(lang)
	for k, m := range messages {
		locale.Messages[k] = m
	}
}
