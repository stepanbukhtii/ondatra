package ondatra

import (
	"database/sql"
	"github.com/shopspring/decimal"
	"time"
)

func NullString(s string) sql.NullString {
	return sql.NullString{
		String: s,
		Valid:  s != "",
	}
}

func NullStringPtr(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{String: "", Valid: false}
	}
	return sql.NullString{String: *s, Valid: true}
}

func StringPtr(s sql.NullString) *string {
	if !s.Valid {
		return nil
	}
	return &s.String
}

func NullInt64(i int64) sql.NullInt64 {
	return sql.NullInt64{Int64: i, Valid: i != 0}
}

func NullInt64Ptr(i *int64) sql.NullInt64 {
	if i == nil {
		return sql.NullInt64{Int64: 0, Valid: false}
	}
	return sql.NullInt64{Int64: *i, Valid: true}
}

func Int64Ptr(i sql.NullInt64) *int64 {
	if !i.Valid {
		return nil
	}
	return &i.Int64
}

func NullInt32(i int32) sql.NullInt32 {
	return sql.NullInt32{Int32: i, Valid: i != 0}
}

func NullInt32Ptr(i *int32) sql.NullInt32 {
	if i == nil {
		return sql.NullInt32{Int32: 0, Valid: false}
	}
	return sql.NullInt32{Int32: *i, Valid: true}
}

func Int32Ptr(i sql.NullInt32) *int32 {
	if !i.Valid {
		return nil
	}
	return &i.Int32
}

func NullInt16(i int16) sql.NullInt16 {
	return sql.NullInt16{Int16: i, Valid: i != 0}
}

func NullInt16Ptr(i *int16) sql.NullInt16 {
	if i == nil {
		return sql.NullInt16{Int16: 0, Valid: false}
	}
	return sql.NullInt16{Int16: *i, Valid: true}
}

func Int16Ptr(i sql.NullInt16) *int16 {
	if !i.Valid {
		return nil
	}
	return &i.Int16
}

func NullByte(b byte) sql.NullByte {
	return sql.NullByte{Byte: b, Valid: b != 0}
}

func NullBytePtr(b *byte) sql.NullByte {
	if b == nil {
		return sql.NullByte{Byte: 0, Valid: false}
	}
	return sql.NullByte{Byte: *b, Valid: true}
}

func BytePtr(i sql.NullByte) *byte {
	if !i.Valid {
		return nil
	}
	return &i.Byte
}

func NullFloat64(f float64) sql.NullFloat64 {
	return sql.NullFloat64{Float64: f, Valid: f != 0}
}

func NullFloat64Ptr(f *float64) sql.NullFloat64 {
	if f == nil {
		return sql.NullFloat64{Float64: 0, Valid: false}
	}
	return sql.NullFloat64{Float64: *f, Valid: true}
}

func Float64Ptr(f sql.NullFloat64) *float64 {
	if !f.Valid {
		return nil
	}
	return &f.Float64
}

func NullDecimal(d decimal.Decimal) decimal.NullDecimal {
	return decimal.NullDecimal{Decimal: d, Valid: !d.IsZero()}
}

func NullDecimalPtr(d *decimal.Decimal) decimal.NullDecimal {
	if d == nil {
		return decimal.NullDecimal{Valid: false}
	}
	return decimal.NullDecimal{Decimal: *d, Valid: true}
}

func DecimalPtr(d decimal.NullDecimal) *decimal.Decimal {
	if !d.Valid {
		return nil
	}
	return &d.Decimal
}

func NullBool(b bool) sql.NullBool {
	return sql.NullBool{Bool: b, Valid: true}
}

func NullBoolPtr(b *bool) sql.NullBool {
	if b == nil {
		return sql.NullBool{Bool: false, Valid: false}
	}
	return sql.NullBool{Bool: *b, Valid: true}
}

func BoolPtr(b sql.NullBool) *bool {
	if !b.Valid {
		return nil
	}
	return &b.Bool
}

func NullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}

func NullTimePtr(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{Time: time.Time{}, Valid: false}
	}
	return sql.NullTime{Time: *t, Valid: true}
}

func TimePtr(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	return &t.Time
}
