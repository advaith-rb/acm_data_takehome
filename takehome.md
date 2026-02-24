# Engineering Take-Home Challenge

## Attendance Prediction Challenge

**Version:** February 2026

---

## Context

This take-home uses **transaction data** and **sentiment/perception data** to estimate whether a customer will attend a soccer match. You will build a prediction model that takes customer features and match context as inputs and outputs an attendance probability with reasoning.

---

## Timebox

Please spend **~3-4 hours total** across the 24 hours you are given with the assignment. 
Use AI tools freely. We’re evaluating engineering judgment, data integration instincts, and product sensibility. While we expect you to generate code using AI tooling (Cursor, Claude Code, etc.), we do expect you to have an understanding of the system implemented and describe design decisions, even if you didn’t write the majority of the code. 

If you cut scope, keep the golden path working and explain tradeoffs in the README.

---

## Goal

Build a small app ("Attendance Prediction Challenge") that lets a user:

- Select a customer
- Configure a match scenario (ticket price, rivalry, day/time, etc.)
- Use transactions + sentiment to construct internal inputs for that customer and scenario
- Run a prediction through a clear model boundary to estimate attendance probability
- Visualize results, show reasoning (what data drove the result), and keep **run history**

You can choose any stack that works for you.

**The purpose of this assignment is to evaluate your ability to build this app. We care about your approach to the prediction problem, not the accuracy of the outputs. We have purposefully given you a generated dataset so you focus on the system design and engineering vs. model tuning.**

---

## Provided Data (Fixtures)

You will receive a `fixtures/` folder containing:

- `transactions.csv` — a vendor-style export of customer transactions
- `sentiment.json` — a feed of social media posts with timestamps and optional engagement
- `customers.csv` — basic customer attributes

Treat these as external datasets: they may be messy (missing values, duplicates, inconsistent timestamp formats). You should not need external API keys.

---

## Design Requirement

**You are free to choose your internal data structures and abstractions.**

We will evaluate whether your design is understandable, extendable, and debuggable.

Your system must demonstrate the invariants below.

---

# A) Data Integration

Your system must:

- Load all provided datasets and transform them into a **unified internal representation** used end-to-end
- Handle missing/invalid values gracefully (don’t crash on bad rows) and deduplicate where needed.
- Compute **customer features** derived from transactions
- Compute **context features** derived from sentiment data.

### Feature examples (can incorporate these and/or choose your own)

- **Customer:** spend frequency/recency, sports affinity (category share), average basket value, price sensitivity proxy
- **Context:** sentiment score, volume, top terms, momentum vs prior window

---

# B) Prediction Model Boundary

- Implement a clear model boundary (e.g., `ModelClient.predict(...)`) that predicts whether a given customer will attend.
- Output must include:
    - attendance probability (0–1)
    - a short explanation (human readable)
- The prediction model should incorporate customer features and context features as structured inputs. A call to `ModelClient.predict()` should accept inputs like:
    - **Customer features**: age group, gender, city, membership tier, sports affinity ratio, average spend, match ticket frequency, favorite team, etc.
    - **Match context**: opponent, date/time, ticket price, venue, rivalry level, etc.
    - **Sentiment context**: team sentiment score, weather sentiment, overall football buzz, etc.
- The approach is up to you — statistical model, ML classifier, heuristic/rule-based system, or any other method. We care about the boundary design, not the specific technique.

# C) UI Requirements

The goal of the UI is to allow the user to predict whether a given customer would attend a soccer game. 

Some suggestions:

- Create basic customer profiles (e.g. segmented by gender, age range, location)
- Allow user to select a customer profile (defined above), configure match parameters (e.g., opponent, date/time, ticket price), and run a prediction
- The prediction should call `ModelClient.predict()` and output
    - Likelihood of attending
    - Reasoning describing this
- User should be able to see previous runs
- (Optional, only if you have time) Allow user to run predictions across multiple customer profiles at once

Feel free to be creative in what you think would be helpful to visualize for such a tool.

---

## D) Backend Requirements (Flexible)

You can design your own API shape. It should support the UI flow above:

- Load/compute features
- Assemble inputs
- Run model boundary
- Return results + provenance

You may implement everything in one service.

---

## Engineering Requirements

- Basic validation and error handling
- You may use whatever tech stack allows you to progress through the project. We don’t expect what was built to be easily scalable/extendable, but we will discuss live how you would approach that given a longer time horizon. Put simply, using local databases, etc. is completely expected.
- README with:
    - How to run
    - What you built
    - Tradeoffs
    - What you’d do next with more time

---

## What to Submit

- Repo link (or zip) containing all code, and README mentioned above
- Any notes you want us to know (optional)

---

## Debrief (1 hour)

In the debrief we’ll:

- Demo the golden path
- Walk through your architecture
- Deep-dive on integration choices
- Discuss how you’d scale/replay/audit the system

---

## Evaluation Criteria

We care about:

- Practical data integration: normalization, dedupe, messy inputs, feature pipeline
- Good boundaries: extendable sources, model swap-in, clear modules
- Provenance and Debuggability end-to-end
- UI Clarity: makes the prediction results understandable; handles loading/error states
- Communication: can explain tradeoffs and risks clearly
- We do NOT care about the accuracy of the model outputs — we care about your approach to the prediction problem, how you structure the model boundary, and whether you incorporate the right features. Do not spend time on model tuning or extensive data science work.