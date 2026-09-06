"""
Confluence Storage Format XHTML to clean Markdown transformation.
Self-contained: pure Python standard library with zero external dependencies.
"""

import html
import re


class ConfluenceMacroSanitizer:
    """
    Transforms Confluence Storage Format XHTML into clean, standard Markdown.
    Preserves high-value code blocks, callouts, tables, and headers while stripping clutter macros.
    """

    @staticmethod
    def sanitize(xhtml_content: str) -> str:
        if not xhtml_content or not isinstance(xhtml_content, str):
            return ""

        text = xhtml_content

        # 1. Transform <ac:structured-macro ac:name="code"> to fenced Markdown
        def replace_code_macro(match):
            macro_str = match.group(0)
            lang_match = re.search(r'<ac:parameter\s+ac:name=["\']language["\']>([^<]+)</ac:parameter>', macro_str, re.DOTALL)
            lang = lang_match.group(1).strip() if lang_match else ""

            # Check CDATA first, then plain-text-body
            cdata_match = re.search(r'<!\[CDATA\[(.*?)\]\]>', macro_str, re.DOTALL)
            if cdata_match:
                code_body = cdata_match.group(1)
            else:
                body_match = re.search(r'<ac:plain-text-body>(.*?)</ac:plain-text-body>', macro_str, re.DOTALL)
                code_body = body_match.group(1) if body_match else ""

            return f"\n```{lang}\n{code_body.strip()}\n```\n"

        text = re.sub(
            r'<ac:structured-macro[^>]*?ac:name=["\']code["\'][^>]*?>.*?</ac:structured-macro>',
            replace_code_macro,
            text,
            flags=re.DOTALL
        )

        # 2. Transform callout macros: info, warning, note, tip
        def replace_callout_macro(match):
            macro_tag = match.group(1).lower()
            macro_body = match.group(2)
            clean_body = re.sub(r'<[^>]+>', ' ', macro_body).strip()
            clean_body = ' '.join(clean_body.split())
            alert_type = "WARNING" if macro_tag in ("warning",) else "NOTE" if macro_tag in ("info", "note") else "TIP"
            return f"\n> [!{alert_type}]\n> {clean_body}\n"

        text = re.sub(
            r'<ac:structured-macro[^>]*?ac:name=["\'](info|warning|note|tip)["\'][^>]*?>.*?<ac:rich-text-body>(.*?)</ac:rich-text-body>.*?</ac:structured-macro>',
            replace_callout_macro,
            text,
            flags=re.DOTALL
        )

        # 3. Strip non-content / clutter macros: toc, profile-card, view-file, details
        text = re.sub(
            r'<ac:structured-macro[^>]*?ac:name=["\'](toc|profile-card|view-file|details)["\'][^>]*?/>',
            '',
            text,
            flags=re.DOTALL
        )
        text = re.sub(
            r'<ac:structured-macro[^>]*?ac:name=["\'](toc|profile-card|view-file|details)["\'][^>/]*?>.*?</ac:structured-macro>',
            '',
            text,
            flags=re.DOTALL
        )

        # 4. Status macro to badge text: [STATUS: APPROVED]
        def replace_status_macro(match):
            title_match = re.search(r'<ac:parameter\s+ac:name=["\']title["\']>([^<]+)</ac:parameter>', match.group(0))
            status_title = title_match.group(1).strip() if title_match else "STATUS"
            return f" [{status_title}] "

        text = re.sub(
            r'<ac:structured-macro[^>]*?ac:name=["\']status["\'][^>]*?>.*?</ac:structured-macro>',
            replace_status_macro,
            text,
            flags=re.DOTALL
        )

        # 5. Transform headings: <h1> to <h6>
        for i in range(6, 0, -1):
            pattern = rf'<h{i}[^>]*>(.*?)</h{i}>'
            replacement = rf'\n{"#" * i} \1\n'
            text = re.sub(pattern, replacement, text, flags=re.DOTALL | re.IGNORECASE)

        # 6. Transform basic paragraph and line breaks
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>(.*?)</p>', r'\n\1\n', text, flags=re.DOTALL | re.IGNORECASE)

        # 7. Convert bold and italics
        text = re.sub(r'<(?:strong|b)[^>]*>(.*?)</(?:strong|b)>', r'**\1**', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<(?:em|i)[^>]*>(.*?)</(?:em|i)>', r'*\1*', text, flags=re.DOTALL | re.IGNORECASE)

        # 8. Unescape standard HTML entities
        text = html.unescape(text)

        # 9. Clean up multiple empty lines
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()
