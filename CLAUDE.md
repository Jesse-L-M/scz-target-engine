## Skill routing

When the user's request matches an available skill, ALWAYS invoke it using the Skill
tool as your FIRST action. Do NOT answer directly, do NOT use other tools first.
The skill has specialized workflows that produce better results than ad-hoc answers.

## Project Continuity

If the task touches schizophrenia program memory, read these first:

- `docs/roadmap.md`
- `docs/designs/program-memory-v3.md`
- `docs/designs/program-memory-v3-resolver.md`
- `docs/program_history.md`
- `docs/claim.md`

The active implementation lane is `program_memory_v3`. The current deterministic
contract is the registered artifact set plus the thin CLI routes for:

- `program-memory harvest-program`
- `program-memory adjudicate-program`
- `program-memory build-insight-packet`

Do not invent parallel dossier shapes or sidecar review files unless the current
contract is explicitly being revised in the same change.

Key routing rules:
- Product ideas, "is this worth building", brainstorming → invoke office-hours
- Bugs, errors, "why is this broken", 500 errors → invoke investigate
- Ship, deploy, push, create PR → invoke ship
- QA, test the site, find bugs → invoke qa
- Code review, check my diff → invoke review
- Update docs after shipping → invoke document-release
- Weekly retro → invoke retro
- Design system, brand → invoke design-consultation
- Visual audit, design polish → invoke design-review
- Architecture review → invoke plan-eng-review
