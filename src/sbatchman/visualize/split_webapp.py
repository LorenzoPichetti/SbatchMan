"""Run once next to webapp.html:  python split_webapp.py
Produces static/{style.css,opentype.js,font.js,app.js,style_addon.js} and a slim webapp.html."""
import re
from pathlib import Path

here = Path(__file__).parent
src = (here / "webapp.html").read_text(encoding="utf-8")
out = here / "static"; out.mkdir(exist_ok=True)

css = re.search(r"<style>(.*?)</style>", src, re.S).group(1)
scripts = re.findall(r"<script>(.*?)</script>", src, re.S)
opentype = next(s for s in scripts if "var opentype=" in s)
app = next(s for s in scripts if "const G = {" in s)
font = re.search(r"const EMBEDDED_SERIF_FONT_TTF_B64 = \"[^\"]*\";", app).group(0)
app = app.replace(font, "// font data lives in font.js")

(out / "style.css").write_text(css.strip(), encoding="utf-8")
(out / "opentype.js").write_text(opentype.strip(), encoding="utf-8")
(out / "font.js").write_text(font, encoding="utf-8")
(out / "app.js").write_text(app.strip(), encoding="utf-8")

body = re.search(r"</style>\s*</head>\s*<body>(.*?)<!-- opentype", src, re.S).group(1)
head = src[: src.index("<style>")]
html = (head + '<link rel="stylesheet" href="/static/style.css">\n</head>\n<body>' + body +
        '\n<script src="/static/opentype.js"></script>\n<script src="/static/font.js"></script>\n'
        '<script src="/static/app.js"></script>\n<script src="/static/style_addon.js"></script>\n</body>\n</html>\n')
(here / "webapp.html").write_text(html, encoding="utf-8")
print("Done. Copy style_addon.js into static/ too.")
