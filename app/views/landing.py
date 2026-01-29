"""
Landing Page
============
Pitch-style landing page for the Supply Chain Visibility & Demand Planning demo.
Tells the story: WHAT, WHY, WHEN, HOW — for executives and planning leaders.
"""

import streamlit as st


def render_landing_page():
    """Render the business-focused landing page for supply chain & demand planning."""

    # Hero Section
    st.markdown(
        """
    <div style="
        background: linear-gradient(135deg, #10212B 0%, #051017 100%);
        border-radius: 16px;
        padding: 48px 40px;
        margin-bottom: 32px;
        position: relative;
        overflow: hidden;
    ">
        <div style="
            position: absolute;
            top: -60px;
            right: -60px;
            width: 320px;
            height: 320px;
            background: radial-gradient(circle, rgba(37, 180, 255, 0.35) 0%, transparent 70%);
            border-radius: 50%;
        "></div>
        <div style="position: relative; z-index: 1;">
            <p style="
                color: #25B4FF;
                font-size: 0.875rem;
                font-weight: 600;
                letter-spacing: 2px;
                text-transform: uppercase;
                margin-bottom: 12px;
                font-family: 'DM Sans', sans-serif;
            ">DATABRICKS FOR SUPPLY CHAIN</p>
            <p style="
                color: #FFFFFF !important;
                font-size: 2.6rem;
                font-weight: 700;
                line-height: 1.15;
                margin: 0 0 16px 0;
                font-family: 'DM Sans', sans-serif;
                text-shadow: 0 2px 4px rgba(0,0,0,0.25);
            ">Demand Planning, Without the Blind Spots</p>
            <p style="
                color: rgba(255,255,255,0.85);
                font-size: 1.2rem;
                font-weight: 400;
                margin: 0;
                max-width: 640px;
                font-family: 'DM Sans', sans-serif;
            ">
                For supply chain and planning leaders at regional, freight-sensitive manufacturers.<br/>
                Unify ERP/WMS/TMS and production signals, scale forecasting to thousands of SKUs,
                and turn forecasts into better service, cost, and sustainability outcomes.
            </p>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Problem Quote Section
    st.markdown(
        """
    <div style="
        background: #25B4FF;
        border-radius: 12px;
        padding: 28px 36px;
        margin-bottom: 32px;
        text-align: center;
    ">
        <p style="
            color: white;
            font-size: 1.4rem;
            font-weight: 500;
            font-style: italic;
            margin: 0 0 10px 0;
            font-family: 'DM Sans', sans-serif;
            line-height: 1.4;
        ">
            "Our planners don’t lack effort — they lack a unified, trustworthy view of demand, inventory, and freight."
        </p>
        <p style="
            color: rgba(255,255,255,0.9);
            font-size: 1rem;
            margin: 0;
            font-family: 'DM Sans', sans-serif;
        ">
            — Common sentiment from supply chain leaders in volatile, project-driven businesses
        </p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Problem Section
    st.markdown(
        """
    <h2 style="
        color: #10212B;
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0 0 18px 0;
        font-family: 'DM Sans', sans-serif;
    ">📊 The Problem: Planning in the Dark is Expensive</h2>
    """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            """
        <div style="
            background: white;
            border: 1px solid #e0e4ea;
            border-radius: 12px;
            padding: 22px;
            text-align: center;
            height: 100%;
        ">
            <p style="
                color: #25B4FF;
                font-size: 2.3rem;
                font-weight: 700;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">20–50%</p>
            <p style="
                color: #4a4f57;
                font-size: 0.9rem;
                margin: 8px 0 0 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Typical forecast error at SKU/location level — driving both<br/>stockouts and excess inventory.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
        <div style="
            background: white;
            border: 1px solid #e0e4ea;
            border-radius: 12px;
            padding: 22px;
            text-align: center;
            height: 100%;
        ">
            <p style="
                color: #25B4FF;
                font-size: 2.3rem;
                font-weight: 700;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">15–25%</p>
            <p style="
                color: #4a4f57;
                font-size: 0.9rem;
                margin: 8px 0 0 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Of inventory value tied up in<br/>slow-moving or excess stock in many supply chains.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            """
        <div style="
            background: white;
            border: 1px solid #e0e4ea;
            border-radius: 12px;
            padding: 22px;
            text-align: center;
            height: 100%;
        ">
            <p style="
                color: #25B4FF;
                font-size: 2.3rem;
                font-weight: 700;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">2–5×</p>
            <p style="
                color: #4a4f57;
                font-size: 0.9rem;
                margin: 8px 0 0 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Premium freight multiples compared to planned transport,<br/>
                when demand surprises hit and buffers are wrong.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)

    # Opportunity Gap Section
    st.markdown(
        """
    <h2 style="
        color: #10212B;
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0 0 20px 0;
        font-family: 'DM Sans', sans-serif;
    ">🎯 The Opportunity Gap: High Stakes, Fragmented Signals</h2>
    """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            """
        <div style="
            background: #10212B;
            border-radius: 12px;
            padding: 22px;
            text-align: center;
        ">
            <p style="
                color: #25B4FF;
                font-size: 2.3rem;
                font-weight: 700;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">77%</p>
            <p style="
                color: white;
                font-size: 0.9rem;
                margin: 8px 0 0 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Of companies are investing in better<br/>supply chain visibility — but struggle to connect it to decisions.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
        <div style="
            background: #10212B;
            border-radius: 12px;
            padding: 22px;
            text-align: center;
        ">
            <p style="
                color: #25B4FF;
                font-size: 2.3rem;
                font-weight: 700;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">20–50%</p>
            <p style="
                color: white;
                font-size: 0.9rem;
                margin: 8px 0 0 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Potential reduction in inventory cost when<br/>
                predictive analytics and better forecasts are applied at scale.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            """
        <div style="
            background: #10212B;
            border-radius: 12px;
            padding: 22px;
            text-align: center;
        ">
            <p style="
                color: #25B4FF;
                font-size: 2.3rem;
                font-weight: 700;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">50%</p>
            <p style="
                color: white;
                font-size: 0.9rem;
                margin: 8px 0 0 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Forecast error reduction reported when<br/>
                AI and real-time data are used in planning.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    st.markdown(
        """
    <div style="
        background: #10212B;
        border-radius: 12px;
        padding: 20px 24px;
        margin: 24px 0 32px 0;
        display: flex;
        align-items: flex-start;
        gap: 12px;
    ">
        <span style="font-size: 1.5rem;">💡</span>
        <div>
            <p style="
                color: white;
                font-size: 1.05rem;
                font-weight: 600;
                margin: 0 0 4px 0;
                font-family: 'DM Sans', sans-serif;
            ">
                This is not a forecasting-effort problem — it’s a data and scale problem.
            </p>
            <p style="
                color: rgba(255,255,255,0.75);
                font-size: 0.95rem;
                font-weight: 400;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Planners are working hard, but ERP, WMS, TMS, telematics, and external signals live in silos,
                making it impossible to trust the numbers or scale to 25k+ SKUs.
            </p>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Shift Section
    st.markdown(
        """
    <h2 style="
        color: #10212B;
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0 0 18px 0;
        font-family: 'DM Sans', sans-serif;
    ">⚡ The Shift: From Reactive Firefighting to Proactive Planning</h2>
    """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
        <div style="
            background: white;
            border: 1px solid #e0e4ea;
            border-radius: 12px;
            padding: 24px;
        ">
            <p style="
                color: #c0392b;
                font-size: 0.875rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 1px;
                margin: 0 0 10px 0;
                font-family: 'DM Sans', sans-serif;
            ">❌ Today’s Reality</p>
            <p style="
                color: #4a4f57;
                font-size: 0.95rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
                line-height: 1.6;
            ">
                Forecasts live in spreadsheets, signals are fragmented, and planners
                <strong>debate whose number is right</strong> while service, freight, and inventory costs drift upward.
                Premium freight becomes the safety net.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
        <div style="
            background: white;
            border: 2px solid #10b981;
            border-radius: 12px;
            padding: 24px;
        ">
            <p style="
                color: #10b981;
                font-size: 0.875rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 1px;
                margin: 0 0 10px 0;
                font-family: 'DM Sans', sans-serif;
            ">✓ With Databricks Lakehouse</p>
            <p style="
                color: #4a4f57;
                font-size: 0.95rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
                line-height: 1.6;
            ">
                Move to a <strong>single, trusted planning backbone</strong> —
                unifying orders, inventory, shipments, and external drivers,
                and using AI to focus planners on the SKUs, regions, and weeks that matter most.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height: 22px;'></div>", unsafe_allow_html=True)

    # Impact Metrics Short List
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            """
        <div style="
            border-left: 3px solid #25B4FF;
            padding-left: 14px;
        ">
            <p style="
                color: #10212B;
                font-size: 1rem;
                font-weight: 600;
                margin: 0 0 4px 0;
                font-family: 'DM Sans', sans-serif;
            ">📦 Service & Reliability</p>
            <p style="
                color: #6b7078;
                font-size: 0.88rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Higher OTIF and fewer surprises in project-driven lanes.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
        <div style="
            border-left: 3px solid #25B4FF;
            padding-left: 14px;
        ">
            <p style="
                color: #10212B;
                font-size: 1rem;
                font-weight: 600;
                margin: 0 0 4px 0;
                font-family: 'DM Sans', sans-serif;
            ">💰 Cost & Working Capital</p>
            <p style="
                color: #6b7078;
                font-size: 0.88rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Leaner buffers, lower premium freight, better inventory turns.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            """
        <div style="
            border-left: 3px solid #25B4FF;
            padding-left: 14px;
        ">
            <p style="
                color: #10212B;
                font-size: 1rem;
                font-weight: 600;
                margin: 0 0 4px 0;
                font-family: 'DM Sans', sans-serif;
            ">🌍 Sustainability & Resilience</p>
            <p style="
                color: #6b7078;
                font-size: 0.88rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
            ">
                Less expedite, better load utilization, and lower CO₂ per ton shipped.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height: 30px;'></div>", unsafe_allow_html=True)

    # How Databricks Does It
    st.markdown(
        """
    <h2 style="
        color: #10212B;
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0 0 20px 0;
        font-family: 'DM Sans', sans-serif;
    ">🚀 How: Supply Chain Planning on Databricks</h2>
    """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
        <div style="
            background: linear-gradient(135deg, #10212B 0%, #051017 100%);
            border-radius: 12px;
            padding: 22px;
            margin-bottom: 14px;
        ">
            <p style="
                color: #25B4FF;
                font-size: 1.1rem;
                font-weight: 600;
                margin: 0 0 6px 0;
                font-family: 'DM Sans', sans-serif;
            ">🗺️ Unified Supply Chain View</p>
            <p style="
                color: rgba(255,255,255,0.85);
                font-size: 0.9rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
                line-height: 1.5;
            ">
                Bring ERP orders, inventory, TMS shipments, telematics-like signals,
                production output, and external demand drivers into one governed lakehouse —
                the foundation for integrated business planning.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

        st.markdown(
            """
        <div style="
            background: linear-gradient(135deg, #10212B 0%, #051017 100%);
            border-radius: 12px;
            padding: 22px;
        ">
            <p style="
                color: #25B4FF;
                font-size: 1.1rem;
                font-weight: 600;
                margin: 0 0 6px 0;
                font-family: 'DM Sans', sans-serif;
            ">📊 Integrated KPIs, One Language</p>
            <p style="
                color: rgba(255,255,255,0.85);
                font-size: 0.9rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
                line-height: 1.5;
            ">
                Track forecast accuracy (MAPE), OTIF, days of supply, inventory turns,
                freight $/ton, premium freight %, and CO₂/ton shipped — all computed
                from the same source of truth that finance and operations can agree on.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
        <div style="
            background: linear-gradient(135deg, #10212B 0%, #051017 100%);
            border-radius: 12px;
            padding: 22px;
            margin-bottom: 14px;
        ">
            <p style="
                color: #25B4FF;
                font-size: 1.1rem;
                font-weight: 600;
                margin: 0 0 6px 0;
                font-family: 'DM Sans', sans-serif;
            ">🤖 AI-Enhanced Forecasting at Scale</p>
            <p style="
                color: rgba(255,255,255,0.85);
                font-size: 0.9rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
                line-height: 1.5;
            ">
                Use the lakehouse to train and govern hierarchical forecasts
                across thousands of SKUs and locations, then surface only the
                true hotspots where planners’ judgment adds the most value.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

        st.markdown(
            """
        <div style="
            background: linear-gradient(135deg, #10212B 0%, #051017 100%);
            border-radius: 12px;
            padding: 22px;
        ">
            <p style="
                color: #25B4FF;
                font-size: 1.1rem;
                font-weight: 600;
                margin: 0 0 6px 0;
                font-family: 'DM Sans', sans-serif;
            ">📱 Decision Apps for Planners</p>
            <p style="
                color: rgba(255,255,255,0.85);
                font-size: 0.9rem;
                margin: 0;
                font-family: 'DM Sans', sans-serif;
                line-height: 1.5;
            ">
                Deliver intuitive apps where leaders and planners can explore scenarios —
                adjust buffers, rebalance inventory, and see the impact on OTIF,
                cost, and emissions in real time.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)

    # CTA Section
    st.markdown(
        """
    <div style="
        background: linear-gradient(135deg, #25B4FF 0%, #1D8CCE 100%);
        border-radius: 12px;
        padding: 30px 36px;
        text-align: center;
    ">
        <p style="
            color: white;
            font-size: 1.45rem;
            font-weight: 600;
            margin: 0 0 6px 0;
            font-family: 'DM Sans', sans-serif;
        ">
            Ready to see the plan in action?
        </p>
        <p style="
            color: rgba(255,255,255,0.9);
            font-size: 1rem;
            margin: 0;
            font-family: 'DM Sans', sans-serif;
        ">
            Use the tabs to explore the Control Tower dashboard, scenario-based value stories,
            and AI-assisted Q&amp;A — all powered by the Databricks Lakehouse.
        </p>
    </div>
    """,
        unsafe_allow_html=True,
    )
