use memchr::memchr2;

fn split_at_eol(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let eol_index = memchr2(b'\n', b'\r', bytes)?;

    Some((
        &bytes[..eol_index],
        match bytes[eol_index] {
            b'\n' => &bytes[eol_index + 1..],
            b'\r' => {
                let potential_new_line_index = eol_index + 1;

                if let Some(b'\n') = bytes.get(potential_new_line_index) {
                    &bytes[eol_index + 2..]
                } else {
                    &bytes[eol_index + 1..]
                }
            }
            _ => return None
        }
    ))
}

enum FieldKind {
    Event,
    Data,
    Id,
    Retry
}

fn parse_field_kind(bytes: &[u8]) -> Option<(FieldKind, &[u8])> {
    Some(
        if let Some(rem) = bytes.strip_prefix(b"event") {
            (FieldKind::Event, rem)
        } else if let Some(rem) = bytes.strip_prefix(b"data") {
            (FieldKind::Data, rem)
        } else if let Some(rem) = bytes.strip_prefix(b"id") {
            (FieldKind::Id, rem)
        } else if let Some(rem) = bytes.strip_prefix(b"retry") {
            (FieldKind::Retry, rem)
        } else {
            return None
        }
    )
}

struct Field<'a> {
    kind: FieldKind,
    value: &'a [u8]
}

fn parse_field_value(bytes: &[u8]) -> Option<&[u8]> {
    Some(match bytes.strip_prefix(b":")? {
        [b' ', rem @ ..] => rem,
        rem @ _ => rem
    })
}

fn parse_field(bytes: &[u8]) -> Option<Field> {
    let (kind, rem) = parse_field_kind(bytes)?;

    Some(Field {
        kind,
        value: parse_field_value(rem)?,
    })
}

enum ParsedLine<'a> {
    Field {
        field: Field<'a>,
        rem: &'a [u8]
    },
    Dispatch,
    Ignored {
        rem: &'a [u8]
    },
    Incomplete
}

pub fn parse_line(buffer: &[u8]) -> ParsedLine {
    let Some((line, rem)) = split_at_eol(buffer) else {
        return ParsedLine::Incomplete
    };

    if line.is_empty() {
        return ParsedLine::Dispatch
    } else if matches!(line.first(), Some(b':')) {
        return ParsedLine::Ignored {
            rem,
        }
    }

    match parse_field(line) {
        None => ParsedLine::Ignored {
            rem,
        },
        Some(field) => ParsedLine::Field {
            field,
            rem,
        }
    }
}