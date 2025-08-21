import re
from typing import Optional


def _is_identifier_start(byte: int) -> bool:
    return (byte == 95) or (65 <= byte <= 90) or (97 <= byte <= 122)


def _is_identifier_part(byte: int) -> bool:
    return _is_identifier_start(byte) or (48 <= byte <= 57) or byte == 36


def rewrite_schema_table(sql: str) -> str:
    s = sql
    i = 0
    n = len(s)
    out = []

    in_squote = False
    in_dquote = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: Optional[str] = None

    while i < n:
        ch = s[i]

        if in_line_comment:
            out.append(ch)
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            out.append(ch)
            if ch == '*' and i + 1 < n and s[i + 1] == '/':
                out.append('/')
                i += 2
                in_block_comment = False
            else:
                i += 1
            continue

        if in_squote:
            out.append(ch)
            if ch == "'":
                if i + 1 < n and s[i + 1] == "'":
                    out.append("'")
                    i += 2
                else:
                    in_squote = False
                    i += 1
            else:
                i += 1
            continue

        if in_dquote:
            out.append(ch)
            if ch == '"':
                if i + 1 < n and s[i + 1] == '"':
                    out.append('"')
                    i += 2
                else:
                    in_dquote = False
                    i += 1
            else:
                i += 1
            continue

        if dollar_tag is not None:
            out.append(ch)
            if ch == '$' and s.startswith(dollar_tag, i):
                out.append(dollar_tag[1:])
                i += len(dollar_tag)
                dollar_tag = None
            else:
                i += 1
            continue

        if ch == '-' and i + 1 < n and s[i + 1] == '-':
            out.append('-')
            out.append('-')
            i += 2
            in_line_comment = True
            continue
        if ch == '/' and i + 1 < n and s[i + 1] == '*':
            out.append('/')
            out.append('*')
            i += 2
            in_block_comment = True
            continue
        if ch == "'":
            out.append(ch)
            in_squote = True
            i += 1
            continue
        if ch == '"':
            j = i + 1
            content_chars = []
            while j < n:
                cj = s[j]
                if cj == '"':
                    if j + 1 < n and s[j + 1] == '"':
                        content_chars.append('"')
                        j += 2
                        continue
                    else:
                        break
                else:
                    content_chars.append(cj)
                    j += 1
            if j < n and s[j] == '"':
                token = ''.join(content_chars)
                if token.count('.') == 1:
                    left, right = token.split('.', 1)
                    if left.isdigit() and (len(right) > 0 and _is_identifier_start(ord(right[0])) and all(_is_identifier_part(ord(c)) for c in right[1:])):
                        out.append('"')
                        out.append(left)
                        out.append('"')
                        out.append('.')
                        out.append('"')
                        out.append(right)
                        out.append('"')
                        i = j + 1
                        continue
                if token.lower() == 'matricula':
                    out.append('"matricula"')
                    i = j + 1
                    continue
                out.append(s[i:j + 1])
                i = j + 1
                continue
            out.append(ch)
            in_dquote = True
            i += 1
            continue
        if ch == '$':
            j = i + 1
            while j < n and (s[j].isalnum() or s[j] == '_'):
                j += 1
            if j < n and s[j] == '$':
                tag = s[i:j + 1]
                out.append(tag)
                i = j + 1
                dollar_tag = tag
                continue

        if s[i].isdigit():
            j = i
            while j < n and s[j].isdigit():
                j += 1
            j_ws = j
            while j_ws < n and s[j_ws].isspace():
                j_ws += 1
            if j_ws < n and s[j_ws] == '.':
                k = j_ws + 1
                while k < n and s[k].isspace():
                    k += 1
                if k < n and _is_identifier_start(ord(s[k])):
                    k2 = k + 1
                    while k2 < n and _is_identifier_part(ord(s[k2])):
                        k2 += 1
                    schema = s[i:j]
                    table = s[k:k2]
                    out.append('"')
                    out.append(schema)
                    out.append('"')
                    out.append('.')
                    out.append('"')
                    out.append(table)
                    out.append('"')
                    i = k2
                    continue
        out.append(ch)
        i += 1

    rewritten = ''.join(out)

    try:
        s_fix = rewritten
        s_fix = re.sub(
            r"(\b[\w$]+\.)(codlig)(\s*=\s*)([\w$]+\.)(?:\"?matricula\"?\b)",
            r'\1\2::text\3\4"matricula"',
            s_fix,
            flags=re.IGNORECASE,
        )
        s_fix = re.sub(
            r"(\b[\w$]+\.)(?:\"?matricula\"?\b)(\s*=\s*)([\w$]+\.)(codlig\b)",
            r'\1"matricula"\2\3\4::text',
            s_fix,
            flags=re.IGNORECASE,
        )
    except Exception:
        return rewritten

    return s_fix


