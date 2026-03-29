"""
Generate AlgoSphere institutional investor overview PDF (presentation only).
Optional: email via ALGOSPHERE_SMTP_USER + ALGOSPHERE_SMTP_PASSWORD (e.g. Gmail app password).
"""

from __future__ import annotations

import argparse
import os
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image as RLImage,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "docs" / "AlgoSphere_Investor_Pitch.pdf"
TO_EMAIL_DEFAULT = "infojobjimmy@gmail.com"

# Institutional palette — Concept 1 Minimal (aligned with Streamlit brand_theme)
GOLD = colors.HexColor("#C9A227")
GOLD_DIM = colors.HexColor("#9A7B1A")
BLUE = colors.HexColor("#2F5BFF")
BG = colors.HexColor("#0D0F12")
CARD = colors.HexColor("#161B22")
TEXT = colors.HexColor("#E6E8EB")
TEXT_MUTED = colors.HexColor("#8A8F98")
LOGO_FILE = ROOT / "frontend" / "assets" / "logo.png"


def _register_fonts() -> tuple[str, str]:
    try:
        pdfmetrics.registerFont(TTFont("DejaVuSans", "DejaVuSans.ttf"))
        pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"))
        body_font = "DejaVuSans"
        bold_font = "DejaVuSans-Bold"
    except Exception:
        body_font = "Helvetica"
        bold_font = "Helvetica-Bold"
    return body_font, bold_font


def _styles(body_font: str, bold_font: str) -> dict:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            name="InvTitle",
            fontName=bold_font,
            fontSize=22,
            leading=28,
            textColor=GOLD,
            alignment=TA_CENTER,
            spaceAfter=8,
        ),
        "subtitle": ParagraphStyle(
            name="InvSub",
            fontName=body_font,
            fontSize=11,
            leading=15,
            textColor=BLUE,
            alignment=TA_CENTER,
            spaceAfter=24,
        ),
        "h1": ParagraphStyle(
            name="InvH1",
            fontName=bold_font,
            fontSize=14,
            leading=18,
            textColor=GOLD,
            alignment=TA_LEFT,
            spaceBefore=16,
            spaceAfter=10,
        ),
        "h2": ParagraphStyle(
            name="InvH2",
            fontName=bold_font,
            fontSize=11,
            leading=14,
            textColor=GOLD_DIM,
            alignment=TA_LEFT,
            spaceBefore=10,
            spaceAfter=6,
        ),
        "body": ParagraphStyle(
            name="InvBody",
            fontName=body_font,
            fontSize=9.5,
            leading=13,
            textColor=TEXT,
            alignment=TA_JUSTIFY,
            spaceAfter=8,
        ),
        "muted": ParagraphStyle(
            name="InvMuted",
            fontName=body_font,
            fontSize=8.5,
            leading=12,
            textColor=TEXT_MUTED,
            alignment=TA_JUSTIFY,
            spaceAfter=6,
        ),
        "footer": ParagraphStyle(
            name="InvFoot",
            fontName=body_font,
            fontSize=8,
            textColor=TEXT_MUTED,
            alignment=TA_CENTER,
        ),
        "brand": ParagraphStyle(
            name="InvBrand",
            fontName=bold_font,
            fontSize=28,
            leading=32,
            textColor=GOLD,
            alignment=TA_CENTER,
            spaceAfter=6,
        ),
    }


def _draw_page_bg(canvas, doc) -> None:
    canvas.saveState()
    canvas.setFillColor(BG)
    canvas.rect(0, 0, LETTER[0], LETTER[1], fill=1, stroke=0)
    # Subtle top rule
    canvas.setStrokeColor(BLUE)
    canvas.setLineWidth(0.5)
    y = LETTER[1] - 0.45 * inch
    canvas.line(0.75 * inch, y, LETTER[0] - 0.75 * inch, y)
    canvas.restoreState()


def _card_table(data: list, col_widths: list | None = None) -> Table:
    t = Table(data, colWidths=col_widths, hAlign="LEFT")
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), CARD),
                ("TEXTCOLOR", (0, 0), (-1, 0), GOLD),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("TEXTCOLOR", (0, 1), (-1, -1), TEXT),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#2D3341")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#12161C"), BG]),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return t


def build_pdf(out_path: Path) -> None:
    body_font, bold_font = _register_fonts()
    st = _styles(body_font, bold_font)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.65 * inch,
        bottomMargin=0.65 * inch,
    )
    story: list = []

    # Cover (logo asset includes full ALGOSPHERE CAPITAL wordmark)
    story.append(Spacer(1, 0.75 * inch))
    if LOGO_FILE.is_file():
        story.append(RLImage(str(LOGO_FILE), width=3.15 * inch, height=1.05 * inch))
        story.append(Spacer(1, 0.35 * inch))
    story.append(Paragraph("Institutional Investor Overview", st["title"]))
    story.append(
        Paragraph(
            "AI-Driven Quantitative Investment Platform",
            st["subtitle"],
        )
    )
    story.append(Spacer(1, 0.5 * inch))
    story.append(
        Paragraph(
            "Confidential · For qualified allocator review · Research &amp; transparency focused",
            st["muted"],
        )
    )
    story.append(PageBreak())

    # 1 Executive Summary
    story.append(Paragraph("1. Executive Summary", st["h1"]))
    story.append(
        Paragraph(
            "AlgoSphere is a <b>research-first, multi-strategy systematic platform</b> designed for "
            "institutional-style evaluation of diversified strategy books across liquid macro and index "
            "exposures. The stack emphasizes <b>risk-managed allocation</b>, transparent methodology, and "
            "<b>AI-assisted</b> research workflows — not retail speculation.",
            st["body"],
        )
    )
    story.append(
        Paragraph(
            "<b>Scope:</b> Multi-asset coverage including precious metals (e.g. XAU), FX, and major equity "
            "indices, supported by approximately <b>ten years</b> of daily historical data for research replay "
            "and portfolio construction discipline.",
            st["body"],
        )
    )
    story.append(
        Paragraph(
            "<b>Posture:</b> Risk-first design with diversification, drawdown awareness, and correlation "
            "controls in the research layer. The approach is <b>institutional</b> in tone: measured claims, "
            "explicit limitations, and separation of research artifacts from live execution policy.",
            st["body"],
        )
    )

    # Executive cards (mini table)
    card_data = [
        ["Research focus", "Multi-family systematic strategies; data-driven scoring"],
        ["Asset classes", "XAU, FX, global indices (illustrative universe)"],
        ["Data horizon", "~10 years daily history (vendor-dependent)"],
        ["Risk philosophy", "Diversification, caps, drawdown discipline"],
    ]
    story.append(Spacer(1, 6))
    story.append(_card_table([["Dimension", "Summary"]] + card_data, [1.6 * inch, 4.7 * inch]))

    # 2 Capital Structure
    story.append(Paragraph("2. Capital Structure", st["h1"]))
    story.append(
        Paragraph(
            "The following is an <b>illustrative</b> capital table for discussion purposes only. "
            "Actual commitments, terms, and closing documents govern any offering.",
            st["muted"],
        )
    )
    cap_rows = [
        ["Investor", "Commitment (USD)"],
        ["Investor 1", "50,000"],
        ["Investor 2", "50,000"],
        ["Investor 3", "100,000"],
        ["Investor 4", "100,000"],
        ["Investor 5", "250,000"],
        ["<b>Total</b>", "<b>550,000</b>"],
    ]
    story.append(_card_table(cap_rows, [3.2 * inch, 2.2 * inch]))

    # 3 Profit Distribution
    story.append(Paragraph("3. Profit Distribution Model", st["h1"]))
    story.append(
        Paragraph(
            "A simplified <b>50% / 50%</b> model is shown below: half of distributable profits allocated to "
            "the platform (AlgoSphere) and half to the investor pool. <b>Investor-side</b> amounts are "
            "allocated <b>by capital weight</b> unless otherwise specified in legal agreements.",
            st["body"],
        )
    )
    story.append(Paragraph("<b>Illustrative split</b>", st["h2"]))
    story.append(_card_table([["Recipient", "Share of profits"], ["AlgoSphere", "50%"], ["Investors (pool)", "50%"]]))
    story.append(Paragraph("<b>Investor pool — weight by commitment</b>", st["h2"]))
    dist_rows = [
        ["Commitment", "Capital weight"],
        ["$50,000", "9.09%"],
        ["$50,000", "9.09%"],
        ["$100,000", "18.18%"],
        ["$100,000", "18.18%"],
        ["$250,000", "45.45%"],
        ["Total", "100.00%"],
    ]
    story.append(_card_table(dist_rows, [2.2 * inch, 2.2 * inch]))

    # 4 Revenue Streams
    story.append(Paragraph("4. Revenue Streams", st["h1"]))
    story.append(
        Paragraph(
            "<b>1. Trading capital (shared):</b> Economics may attach to pooled or segregated trading capital "
            "subject to fund documents; any split is contractual, not implied by software alone.",
            st["body"],
        )
    )
    story.append(
        Paragraph(
            "<b>2. Subscription platform:</b> SaaS-style access to dashboards and research views — "
            "typically <b>100% AlgoSphere</b> revenue line.",
            st["body"],
        )
    )
    story.append(
        Paragraph(
            "<b>3. Licensing / technology:</b> API, white-label, or deployment licenses — "
            "<b>100% AlgoSphere</b> unless a revenue-share is contractually agreed.",
            st["body"],
        )
    )
    story.append(
        Paragraph(
            "<b>4. AI research platform (future):</b> Institutional research tooling, allocator reporting, "
            "and workflow automation — potential incremental recurring revenue.",
            st["body"],
        )
    )

    # 5 Risk Management
    story.append(Paragraph("5. Risk Management", st["h1"]))
    story.append(
        Paragraph(
            "• <b>Multi-strategy diversification</b> across families to reduce single-factor dominance.<br/>"
            "• <b>Risk-capped portfolios</b> and quota-style construction in research books.<br/>"
            "• <b>Drawdown controls</b> embedded in evaluation metrics and screening.<br/>"
            "• <b>AI-assisted risk allocation</b> as decision support — not autonomous capital deployment "
            "without governance.<br/>"
            "• <b>No over-leverage</b> as a design principle; explicit leverage requires policy and documentation.",
            st["body"],
        )
    )

    # 6 Technology Vision
    story.append(Paragraph("6. Technology Vision", st["h1"]))
    story.append(
        Paragraph(
            "AlgoSphere’s roadmap centers on a disciplined <b>AI allocation engine</b>, "
            "<b>institutional-grade</b> reporting, a <b>multi-asset global</b> research footprint, and "
            "<b>scalable infrastructure</b> suitable for growing allocator interest — without compromising "
            "transparency or risk governance.",
            st["body"],
        )
    )

    # 7 Investor Benefits
    story.append(Paragraph("7. Investor Benefits", st["h1"]))
    story.append(
        Paragraph(
            "• <b>Passive exposure</b> (where structured via fund terms) to a systematic research process.<br/>"
            "• <b>Transparent dashboard</b> and allocator-oriented materials for due diligence.<br/>"
            "• <b>Performance tracking</b> framed as research replay and risk metrics — not marketing guarantees.<br/>"
            "• <b>Institutional structure</b> alignment in documentation, governance, and professional presentation.",
            st["body"],
        )
    )

    # 8 Legal
    story.append(Paragraph("8. Legal &amp; Risk Disclaimer", st["h1"]))
    story.append(
        Paragraph(
            "This document is <b>not an offer</b> to sell or a solicitation to buy any security or fund "
            "interest. Past performance and historical research replay <b>are not indicative of future "
            "results</b>. Markets involve substantial risk of loss. AlgoSphere materials are "
            "<b>research-based</b> and may rely on third-party data with known limitations. "
            "Prospective investors should consult legal, tax, and financial advisors. "
            "<b>No guarantee of returns</b> is made or implied.",
            st["body"],
        )
    )
    story.append(Spacer(1, 16))
    story.append(Paragraph("© AlgoSphere · Institutional overview · Presentation only", st["footer"]))

    doc.build(story, onFirstPage=_draw_page_bg, onLaterPages=_draw_page_bg)


def send_email(pdf_path: Path, to_addr: str) -> tuple[bool, str]:
    load_dotenv(ROOT / ".env")
    user = os.getenv("ALGOSPHERE_SMTP_USER", "").strip()
    password = os.getenv("ALGOSPHERE_SMTP_PASSWORD", "").strip()
    host = os.getenv("ALGOSPHERE_SMTP_HOST", "smtp.gmail.com").strip()
    port = int(os.getenv("ALGOSPHERE_SMTP_PORT", "465"))
    from_addr = os.getenv("ALGOSPHERE_SMTP_FROM", user).strip()

    if not user or not password:
        return False, (
            "Email not sent: set ALGOSPHERE_SMTP_USER and ALGOSPHERE_SMTP_PASSWORD in .env "
            "(e.g. Gmail app password). Network SMTP required."
        )

    subject = "AlgoSphere Capital — Investor Overview"
    body = (
        "Dear Investor,\n\n"
        "Please find attached the AlgoSphere Capital institutional investor overview — "
        "a concise introduction to our AI-driven multi-strategy research and capital allocation platform, "
        "risk management posture, and illustrative capital framework.\n\n"
        "This material is confidential and intended for qualified allocator review. "
        "It is not an offer or solicitation.\n\n"
        "Best regards,\n"
        "AlgoSphere Capital\n"
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(body)
    msg.add_attachment(
        pdf_path.read_bytes(),
        maintype="application",
        subtype="pdf",
        filename=pdf_path.name,
    )

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(host, port, context=context) as server:
        server.login(user, password)
        server.send_message(msg)

    return True, f"Sent to {to_addr} via {host}:{port}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--email", action="store_true", help="Send PDF if SMTP env vars set")
    parser.add_argument("--to", default=TO_EMAIL_DEFAULT)
    args = parser.parse_args()

    build_pdf(args.out)
    print(f"PDF written: {args.out}")

    if args.email:
        ok, status = send_email(args.out, args.to)
        print(status)
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
