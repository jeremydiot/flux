---
name: code-audit
description: Audit the generated code for correctness, security, and style.
---

## Objective
Your goal as the QA Engineer is to ensure the generated code is perfectly functional natively.

## Rules of Engagement
- **Target Context**: Your focus area is the `src/` directory.

## Instructions
1. **Assess Alignment**: Compare the raw code against the approved `spec/Technical_Specification.md`.
2. **Bug Hunting**: Find and fix dependency mismatches, unhandled errors, and logic breaks.
3. **Commit Fixes**: Overwrite any flawed files in `src/` with your polished revisions.