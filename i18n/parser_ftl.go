package i18n

import (
	"bufio"
	"strings"
)

// FTLParser parses Fluent (FTL) format locale content into Message maps.
type FTLParser struct{}

// NewFTLParser returns a new FTLParser.
func NewFTLParser() *FTLParser {
	return &FTLParser{}
}

// Parse reads FTL content and returns a map of message keys to Message values.
// Comments (lines starting with #) and blank lines are skipped.
func (p *FTLParser) Parse(content string) (map[string]*Message, error) {
	messages := make(map[string]*Message)
	scanner := bufio.NewScanner(strings.NewReader(content))
	var currentKey string
	var currentValue strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.Contains(line, "=") && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			if currentKey != "" {
				messages[currentKey] = &Message{
					Key:   currentKey,
					Value: strings.TrimSpace(currentValue.String()),
				}
			}
			parts := strings.SplitN(line, "=", 2)
			currentKey = strings.TrimSpace(parts[0])
			currentValue.Reset()
			currentValue.WriteString(strings.TrimSpace(parts[1]))
		} else if strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "\t") {
			if currentKey != "" {
				if currentValue.Len() > 0 {
					currentValue.WriteString(" ")
				}
				currentValue.WriteString(strings.TrimSpace(line))
			}
		}
	}
	if currentKey != "" {
		messages[currentKey] = &Message{
			Key:   currentKey,
			Value: strings.TrimSpace(currentValue.String()),
		}
	}
	return messages, nil
}
