package updatesrecovery

import (
	"reflect"

	"github.com/mtgo-labs/mtgo/tg"
)

// gapKind classifies the relationship between an incoming update's sequence
// numbers and the stored state.
type gapKind int

const (
	gapNone      gapKind = iota // update is in sequence
	gapDuplicate                // update was already applied
	gapAccount                  // pts/qts gap — updates were missed
	gapSeq                      // seq_start gap — batched updates were missed
)

// updateInfo holds the sequence-relevant fields extracted from an update batch
// or individual update.
type updateInfo struct {
	pts       int32
	ptsCount  int32
	qts       int32
	seq       int32
	seqStart  int32
	date      int32
	channelID int64
}

// extractBatch extracts update info from a top-level tg.UpdatesClass.
// For Updates/UpdatesCombined it flattens to individual updates and computes
// the aggregate pts. For UpdateShort* it reads the struct directly.
// Returns the info, whether a gap-recovery signal was detected (UpdatesTooLong),
// and the flattened list of individual updates (nil for short types).
func extractBatch(updates tg.UpdatesClass) (info updateInfo, tooLong bool, items []tg.UpdateClass) {
	switch v := updates.(type) {
	case *tg.UpdatesTooLong:
		return updateInfo{}, true, nil

	case *tg.Updates:
		info.date = v.Date
		info.seq = v.Seq
		return info, false, v.Updates

	case *tg.UpdatesCombined:
		info.date = v.Date
		info.seq = v.Seq
		info.seqStart = v.SeqStart
		return info, false, v.Updates

	case *tg.UpdateShort:
		// Single update with a date stamp.
		info.date = v.Date
		meta := extractUpdateMeta(v.Update)
		mergeMeta(&info, &meta)
		return info, false, nil

	case *tg.UpdateShortMessage:
		info.pts = v.PTS
		info.ptsCount = v.PTSCount
		info.date = v.Date
		return info, false, nil

	case *tg.UpdateShortChatMessage:
		info.pts = v.PTS
		info.ptsCount = v.PTSCount
		info.date = v.Date
		return info, false, nil

	case *tg.UpdateShortSentMessage:
		info.pts = v.PTS
		info.ptsCount = v.PTSCount
		info.date = v.Date
		return info, false, nil

	default:
		return updateInfo{}, false, nil
	}
}

// extractUpdateMeta reads pts/ptsCount/qts/seq from a single UpdateClass using
// reflection. Telegram TL structs consistently name these fields PTS, PTSCount,
// QTS, Seq, SeqStart — reflection avoids a 100-case type switch.
func extractUpdateMeta(upd tg.UpdateClass) updateInfo {
	var info updateInfo
	if upd == nil {
		return info
	}
	v := reflect.ValueOf(upd)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return info
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return info
	}
	info.pts = readInt32(v, "PTS")
	info.ptsCount = readInt32(v, "PTSCount")
	info.qts = readInt32(v, "QTS")
	info.seq = readInt32(v, "Seq")
	info.seqStart = readInt32(v, "SeqStart")
	info.channelID = readInt64(v, "ChannelID")
	return info
}

func mergeMeta(dst, src *updateInfo) {
	if src.pts != 0 {
		dst.pts = src.pts
	}
	if src.ptsCount != 0 {
		dst.ptsCount += src.ptsCount
	}
	if src.qts != 0 {
		dst.qts = src.qts
	}
	if src.seq != 0 {
		dst.seq = src.seq
	}
	if src.seqStart != 0 {
		dst.seqStart = src.seqStart
	}
	if src.date != 0 {
		dst.date = src.date
	}
}

// classifyAccount compares an incoming update's sequence numbers against the
// stored state and returns whether a gap was detected.
//
// For pts-bearing updates: expected = state.pts + ptsCount. If incoming pts
// equals expected, it's in sequence. If <= state.pts, it's a duplicate.
// Otherwise there's a gap.
//
// For qts-bearing updates (no pts): if qts > state.qts + 1, there's a gap.
//
// For seq-bearing updates (no pts/qts): if seq > state.seq + 1, there's a gap.
func classifyAccount(state State, info updateInfo) gapKind {
	if info.pts > 0 {
		expected := state.Pts + info.ptsCount
		switch {
		case info.pts == expected:
			return gapNone
		case info.pts <= state.Pts:
			return gapDuplicate
		default:
			return gapAccount
		}
	}
	if info.qts > 0 {
		if info.qts <= state.Qts {
			return gapDuplicate
		}
		if info.qts > state.Qts+1 {
			return gapAccount
		}
		return gapNone
	}
	if info.seq > 0 {
		if info.seq <= state.Seq {
			return gapDuplicate
		}
		if info.seq > state.Seq+1 {
			return gapAccount
		}
		return gapNone
	}
	return gapNone
}

// classifySeq checks the seq/seqStart fields of an incoming batch against the
// stored state. For UpdatesCombined, seqStart is compared against state.Seq+1.
// For plain Updates (no seqStart), seq is compared directly.
func classifySeq(state State, info updateInfo) gapKind {
	if info.seq == 0 {
		return gapNone
	}
	if info.seqStart > 0 {
		switch {
		case info.seqStart > state.Seq+1:
			return gapSeq
		case info.seqStart <= state.Seq:
			return gapDuplicate
		default:
			return gapNone
		}
	}
	switch {
	case info.seq > state.Seq+1:
		return gapSeq
	case info.seq <= state.Seq:
		return gapDuplicate
	default:
		return gapNone
	}
}

func readInt32(v reflect.Value, name string) int32 {
	f := v.FieldByName(name)
	if !f.IsValid() {
		return 0
	}
	if f.Kind() == reflect.Int32 {
		return int32(f.Int())
	}
	return 0
}

func readInt64(v reflect.Value, name string) int64 {
	f := v.FieldByName(name)
	if !f.IsValid() {
		return 0
	}
	if f.Kind() == reflect.Int64 {
		return f.Int()
	}
	return 0
}
