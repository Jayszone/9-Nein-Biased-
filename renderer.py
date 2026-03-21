import datetime

def get_bias_label(bias):
    labels = {"left": "Left-Leaning", "center": "Center/Balanced", "right": "Right-Leaning"}
    return labels.get(bias, bias.capitalize())

def get_bias_color(bias):
    colors = {"left": "#4A90D9", "center": "#6B7280", "right": "#E05C5C"}
    return colors.get(bias, "#6B7280")

def get_bias_bg(bias):
    bgs = {"left": "#EBF4FF", "center": "#F3F4F6", "right": "#FEF2F2"}
    return bgs.get(bias, "#F3F4F6")

def score_to_label(score):
    if score >= 0.75:
        return "Analytical"
    elif score >= 0.5:
        return "Mostly Analytical"
    elif score >= 0.25:
        return "Mostly Opinion"
    else:
        return "Opinion / Editorial"

def score_to_color(score):
    if score >= 0.75:
        return "#059669"
    elif score >= 0.5:
        return "#D97706"
    elif score >= 0.25:
        return "#EA580C"
    else:
        return "#DC2626"

def render_quadrant_chart(article_scores):
    """Render a visual 3-column (left | center | right) x 2-row (analytical | opinion) chart."""
    left_analytical, left_opinion = [], []
    center_analytical, center_opinion = [], []
    right_analytical, right_opinion = [], []

    for a in article_scores:
        bias = a.get("source_bias", "center")
        score = a.get("opinion_vs_analysis_score", 0.5)
        name = a.get("source_name", "Unknown")
        label = (
            f'<span style="font-size:11px;font-weight:600;color:#1F2937">{name}</span>'
            f'<br><span style="font-size:10px;color:#6B7280">{score_to_label(score)}</span>'
        )

        if bias == "left":
            (left_analytical if score >= 0.5 else left_opinion).append(label)
        elif bias == "right":
            (right_analytical if score >= 0.5 else right_opinion).append(label)
        else:
            (center_analytical if score >= 0.5 else center_opinion).append(label)

    def cell(items, bg, border):
        content = "<br><br>".join(items) if items else '<span style="color:#9CA3AF;font-size:11px">—</span>'
        return (
            f'<td style="padding:14px;background:{bg};border:{border};'
            f'vertical-align:top;border-radius:4px">{content}</td>'
        )

    html = f'''
    <table width="100%" cellpadding="0" cellspacing="0" style="margin:16px 0">
      <tr>
        <td colspan="5" style="text-align:center;padding-bottom:6px">
          <span style="font-size:11px;font-weight:700;color:#059669;letter-spacing:0.08em;text-transform:uppercase">▲ Analytical / Factual</span>
        </td>
      </tr>
      <tr>
        <td width="32%" style="text-align:center;padding-bottom:4px">
          <span style="font-size:11px;font-weight:700;color:#4A90D9;letter-spacing:0.06em;text-transform:uppercase">◀ Left</span>
        </td>
        <td width="2%"></td>
        <td width="32%" style="text-align:center;padding-bottom:4px">
          <span style="font-size:11px;font-weight:700;color:#6B7280;letter-spacing:0.06em;text-transform:uppercase">Center</span>
        </td>
        <td width="2%"></td>
        <td width="32%" style="text-align:center;padding-bottom:4px">
          <span style="font-size:11px;font-weight:700;color:#E05C5C;letter-spacing:0.06em;text-transform:uppercase">Right ▶</span>
        </td>
      </tr>
      <tr>
        {cell(left_analytical,   "#F0F7FF", "2px solid #4A90D9")}
        <td></td>
        {cell(center_analytical, "#F3F4F6", "2px solid #6B7280")}
        <td></td>
        {cell(right_analytical,  "#FFF5F5", "2px solid #E05C5C")}
      </tr>
      <tr><td colspan="5" style="height:6px"></td></tr>
      <tr>
        {cell(left_opinion,   "#E8F2FF", "2px dashed #4A90D9")}
        <td></td>
        {cell(center_opinion, "#EBEBEC", "2px dashed #6B7280")}
        <td></td>
        {cell(right_opinion,  "#FFEDED", "2px dashed #E05C5C")}
      </tr>
      <tr>
        <td colspan="5" style="text-align:center;padding-top:6px">
          <span style="font-size:11px;font-weight:700;color:#DC2626;letter-spacing:0.08em;text-transform:uppercase">▼ Opinion / Editorial</span>
        </td>
      </tr>
    </table>
    '''
    return html

def render_story_block(story_num, story, analysis):
    article_scores = analysis.get("article_scores", [])
    factual_core = analysis.get("factual_core", [])
    framing_contrast = analysis.get("framing_contrast", "")

    # Sort: center first, then left, then right
    bias_order = {"center": 0, "left": 1, "right": 2}
    article_scores = sorted(article_scores, key=lambda a: bias_order.get(a.get("source_bias", "center"), 1))

    # Article pills
    article_pills = ""
    for a in article_scores:
        bias = a.get("source_bias", "center")
        score = a.get("opinion_vs_analysis_score", 0.5)
        bg = get_bias_bg(bias)
        color = get_bias_color(bias)
        score_color = score_to_color(score)
        article_pills += f'''
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #F3F4F6">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <span style="display:inline-block;padding:2px 8px;background:{bg};color:{color};border-radius:20px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em">{get_bias_label(bias)}</span>
                  &nbsp;
                  <span style="font-size:12px;font-weight:600;color:#1F2937">{a.get("source_name","")}</span>
                </td>
                <td align="right">
                  <span style="font-size:11px;color:{score_color};font-weight:600">{score_to_label(score)} ({int(score*100)}%)</span>
                </td>
              </tr>
              <tr>
                <td colspan="2" style="padding-top:2px">
                  <span style="font-size:11px;color:#6B7280;font-style:italic">{a.get("score_reasoning","")}</span>
                </td>
              </tr>
            </table>
          </td>
        </tr>'''

    # Factual core bullets
    facts_html = ""
    for fact in factual_core:
        facts_html += f'<li style="margin-bottom:6px;font-size:13px;color:#374151;line-height:1.5">{fact}</li>'

    quadrant = render_quadrant_chart(article_scores)

    return f'''
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:40px">
      <tr>
        <td style="padding-bottom:8px">
          <table cellpadding="0" cellspacing="0">
            <tr>
              <td style="background:#111827;color:#F9FAFB;font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;padding:4px 10px;border-radius:3px">
                STORY {story_num}
              </td>
            </tr>
          </table>
        </td>
      </tr>
      <tr>
        <td style="padding-bottom:16px">
          <h2 style="margin:0;font-family:'Georgia',serif;font-size:22px;font-weight:700;color:#111827;line-height:1.35">{story["headline"]}</h2>
        </td>
      </tr>

      <!-- Quadrant Chart -->
      <tr>
        <td style="padding-bottom:6px">
          <span style="font-size:11px;font-weight:700;color:#6B7280;letter-spacing:0.08em;text-transform:uppercase">Coverage Map</span>
        </td>
      </tr>
      <tr><td>{quadrant}</td></tr>

      <!-- Source breakdown -->
      <tr>
        <td style="padding-top:16px;padding-bottom:6px">
          <span style="font-size:11px;font-weight:700;color:#6B7280;letter-spacing:0.08em;text-transform:uppercase">Source Breakdown</span>
        </td>
      </tr>
      <tr>
        <td>
          <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #E5E7EB;border-radius:6px;overflow:hidden">
            {article_pills}
          </table>
        </td>
      </tr>

      <!-- Factual Core -->
      <tr>
        <td style="padding-top:20px;padding-bottom:6px">
          <span style="font-size:11px;font-weight:700;color:#6B7280;letter-spacing:0.08em;text-transform:uppercase">✓ Shared Factual Core</span>
        </td>
      </tr>
      <tr>
        <td style="background:#F0FDF4;border-left:4px solid #059669;padding:16px 20px;border-radius:0 6px 6px 0">
          <ul style="margin:0;padding-left:18px">
            {facts_html}
          </ul>
        </td>
      </tr>

      <!-- Framing Contrast -->
      <tr>
        <td style="padding-top:16px;padding-bottom:6px">
          <span style="font-size:11px;font-weight:700;color:#6B7280;letter-spacing:0.08em;text-transform:uppercase">↔ Framing Contrast</span>
        </td>
      </tr>
      <tr>
        <td style="background:#FFFBEB;border-left:4px solid #D97706;padding:16px 20px;border-radius:0 6px 6px 0">
          <p style="margin:0;font-size:13px;color:#374151;line-height:1.6;font-style:italic">{framing_contrast}</p>
        </td>
      </tr>
    </table>
    '''


def render_newsletter(stories_with_analysis):
    today = datetime.date.today().strftime("%B %d, %Y")
    day_of_week = datetime.date.today().strftime("%A")

    story_blocks = ""
    for i, (story, analysis) in enumerate(stories_with_analysis, 1):
        story_blocks += render_story_block(i, story, analysis)
        if i < len(stories_with_analysis):
            story_blocks += '<hr style="border:none;border-top:1px solid #E5E7EB;margin:32px 0">'

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>9 (Nein) Biased — {today}</title>
</head>
<body style="margin:0;padding:0;background:#F9FAFB;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">

  <table width="100%" cellpadding="0" cellspacing="0" style="background:#F9FAFB;padding:32px 16px">
    <tr>
      <td align="center">
        <table width="620" cellpadding="0" cellspacing="0" style="max-width:620px;width:100%">

          <!-- HEADER -->
          <tr>
            <td style="background:#111827;padding:32px 36px;border-radius:8px 8px 0 0">
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td>
                    <div style="font-family:'Georgia',serif;font-size:32px;font-weight:700;color:#F9FAFB;letter-spacing:-0.5px">9 <span style="color:#E05C5C">(Nein)</span> Biased</div>
                    <div style="font-size:12px;color:#9CA3AF;margin-top:4px;letter-spacing:0.06em;text-transform:uppercase">See All Sides. Know the Facts.</div>
                  </td>
                  <td align="right" style="vertical-align:top">
                    <div style="font-size:11px;color:#6B7280;text-align:right">{day_of_week}</div>
                    <div style="font-size:13px;color:#D1D5DB;font-weight:600;text-align:right">{today}</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- LEGEND BAR -->
          <tr>
            <td style="background:#1F2937;padding:12px 36px">
              <table cellpadding="0" cellspacing="0">
                <tr>
                  <td style="padding-right:20px">
                    <span style="display:inline-block;width:10px;height:10px;background:#4A90D9;border-radius:2px;margin-right:5px;vertical-align:middle"></span>
                    <span style="font-size:11px;color:#9CA3AF">Left-Leaning</span>
                  </td>
                  <td style="padding-right:20px">
                    <span style="display:inline-block;width:10px;height:10px;background:#6B7280;border-radius:2px;margin-right:5px;vertical-align:middle"></span>
                    <span style="font-size:11px;color:#9CA3AF">Center/Balanced</span>
                  </td>
                  <td style="padding-right:20px">
                    <span style="display:inline-block;width:10px;height:10px;background:#E05C5C;border-radius:2px;margin-right:5px;vertical-align:middle"></span>
                    <span style="font-size:11px;color:#9CA3AF">Right-Leaning</span>
                  </td>
                  <td>
                    <span style="font-size:11px;color:#9CA3AF">Solid border = Analytical · Dashed = Opinion</span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- INTRO -->
          <tr>
            <td style="background:#ffffff;padding:24px 36px;border-bottom:1px solid #E5E7EB">
              <p style="margin:0;font-size:13px;color:#6B7280;line-height:1.6">
                Today's top <strong style="color:#111827">3 stories</strong> — analyzed across {sum(len(s["articles"]) for s, _ in stories_with_analysis)} sources. Each story shows where outlets fall on the political spectrum and how analytical vs. opinion-driven their coverage is. The <span style="color:#059669;font-weight:600">Shared Factual Core</span> highlights what all sources agree on.
              </p>
            </td>
          </tr>

          <!-- STORY BLOCKS -->
          <tr>
            <td style="background:#ffffff;padding:32px 36px">
              {story_blocks}
            </td>
          </tr>

          <!-- FOOTER -->
          <tr>
            <td style="background:#111827;padding:24px 36px;border-radius:0 0 8px 8px">
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td>
                    <div style="font-family:'Georgia',serif;font-size:16px;color:#F9FAFB;font-weight:700">9 (Nein) Biased</div>
                    <div style="font-size:11px;color:#6B7280;margin-top:4px">Bias classifications are based on established media research. Opinion vs. Analysis scores are AI-generated and may not be perfect. We recommend reading primary sources.</div>
                  </td>
                  <td align="right" style="vertical-align:top">
                    <div style="font-size:11px;color:#6B7280">Unsubscribe</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>

</body>
</html>'''

    return html
