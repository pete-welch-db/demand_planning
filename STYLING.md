# STYLING.md (Shared SA Demo Standards)

This file is the **single source of truth** for styling across SA-built demos. Apps should **copy the CSS variables + component classes** from here into `app/components/styles.py` and use the helper functions in `app/components/metrics.py` for Plotly theming.

## Palette (Databricks-style)

- **Backgrounds**
  - Oat Medium: `#EEEDE9` (page background)
  - Oat Light: `#F9F7F4` (sidebar / alternate sections)
- **Cards**
  - White: `#FFFFFF`
  - Border: `#e0dfdc`
  - Radius: 8–12px
  - Shadow: subtle only
- **Accents**
  - Lava 600: `#FF3621` (primary CTA / active)
  - Lava 500: `#FF5F46` (hover)
  - Navy 900/800: secondary emphasis

## Typography

- **Font**: DM Sans (Google Fonts)
- **Sizes/weights**
  - Title/Hero: 32–40px, weight 700
  - Section headers: 24px, weight 600
  - Body: 16px, weight 400
  - Labels: 14px, weight 500

## Required CSS variables block (copy into apps)

```css
:root{
  --lava-600:#FF3621;
  --lava-500:#FF5F46;
  --navy-900:#0B1220;
  --navy-800:#111C33;

  --bg-primary:#EEEDE9;   /* Oat Medium */
  --bg-secondary:#F9F7F4; /* Oat Light  */
  --card-bg:#FFFFFF;
  --card-border:#e0dfdc;

  --text-primary:#111827;
  --text-secondary:rgba(17, 24, 39, 0.72);
  --grid:rgba(17, 24, 39, 0.10);
  --shadow:0 1px 2px rgba(16,24,40,0.06);
  --radius:10px;
  --spacing:8px;
}
```

## Core components

### Branded header

Use a header that contains:
- Databricks logo (SVG in `app/assets/`)
- App name
- Optional right-side “environment / data source” pills

### Landing hero + value cards (required for all demos)

Every demo should open with:
- **Hero**: page title + 2-line narrative for the persona
- **Value cards**: Problem, Data, KPIs, Impact
- **How it works**: ingest & unify → ML/Genie/AI → decision & action

Recommended classes (copy into apps):

```css
.hero{
  background: var(--bg-secondary);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 18px 18px;
  margin-bottom: 14px;
}
.hero-title{
  font-size: 36px;
  font-weight: 700;
  color: var(--navy-900);
  line-height: 1.05;
  margin: 0 0 6px 0;
}
.hero-narrative{
  font-size: 16px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
  margin: 0;
}
.section-title{
  font-size: 24px;
  font-weight: 600;
  color: var(--navy-900);
  margin: 14px 0 10px 0;
}
.value-card{
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 14px 14px;
}
.value-card-title{
  font-size: 14px;
  font-weight: 600;
  color: var(--navy-800);
  margin-bottom: 8px;
}
.value-card-body{
  font-size: 16px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
}
.how-step{
  background: var(--bg-secondary);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 14px 14px;
}
.how-step-title{
  font-size: 16px;
  font-weight: 600;
  color: var(--navy-900);
  margin-bottom: 6px;
}
.how-step-body{
  font-size: 14px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
}
```

### Tab narrative components (required for every tab)

If a tab has charts but no narrative, it’s unfinished.

```css
.tab-intro{
  background: var(--bg-secondary);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 14px 14px;
  margin: 0 0 14px 0;
}
.tab-intro-persona{
  font-size: 14px;
  font-weight: 600;
  color: var(--navy-800);
  margin-bottom: 6px;
}
.tab-intro-question{
  font-size: 18px;
  font-weight: 700;
  color: var(--navy-900);
  margin-bottom: 6px;
}
.tab-intro-context{
  font-size: 14px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
}

.callout{
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 12px 14px;
  margin: 10px 0;
}
.callout-title{
  font-size: 14px;
  font-weight: 700;
  color: var(--navy-900);
  margin-bottom: 6px;
}
.callout-body{
  font-size: 14px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
}
.callout-annot{
  background: #FFFFFF;
  border-left: 4px solid var(--navy-800);
}
.callout-action{
  background: #FFFFFF;
  border-left: 4px solid var(--lava-600);
}
```

### Metric cards (must reuse these classes)

```css
.metric-card{
  background:var(--card-bg);
  border:1px solid var(--card-border);
  border-radius:var(--radius);
  box-shadow:var(--shadow);
  padding:12px 14px;
}
.metric-label{
  font-size:14px;
  font-weight:500;
  color:var(--text-secondary);
  margin-bottom:6px;
}
.metric-value{
  font-size:24px;
  font-weight:700;
  color:var(--text-primary);
  line-height:1.2;
}
.metric-delta{
  margin-top:6px;
  font-size:14px;
  font-weight:600;
}
.metric-delta.positive{ color:#067647; }
.metric-delta.negative{ color:#B42318; }
```

### Buttons (primary + outline)

```css
div.stButton > button{
  border-radius:10px !important;
  font-weight:600 !important;
  border:1px solid transparent !important;
  background:var(--lava-600) !important;
  color:white !important;
}
div.stButton > button:hover{
  background:var(--lava-500) !important;
}
div.stButton.secondary > button{
  background:transparent !important;
  border:1px solid var(--card-border) !important;
  color:var(--navy-800) !important;
}
```

### Tabs (pill-style active = Lava)

Include the tab CSS in apps even if not used in every demo; it keeps dashboards consistent.

### Plotly charts

Use `components/metrics.py:create_plotly_theme()`:
- transparent paper/plot background
- DM Sans
- branded colorway
- soft grids

## Don’t

- Don’t use default Streamlit theme colors.
- Don’t use pure black text or heavy shadows.
- Don’t mix fonts.
- Don’t use Plotly `font.weight` (not supported).

