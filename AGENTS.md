# Working agreement

- Write and maintain project documentation in English, including source-code comments, docstrings, test descriptions, and user-facing demo text.
- Add docstrings to public modules, classes, and functions when their purpose or contract is not obvious. State relevant inputs, outputs, side effects, failure behavior, and invariants; do not restate the signature or narrate each line of implementation.
- Use comments to explain non-obvious reasoning, constraints, or trade-offs. Prefer clear names and structure over comments that merely translate the code into prose.
- Update relevant documentation when changing behavior, contracts, configuration, or operational procedures. Keep each document focused on its source of truth and link to related material instead of copying it.
- Keep examples and operational instructions runnable and aligned with the current implementation. Never include secrets or real payment/customer data.
- Treat a completed, independently testable functional block as a commit boundary.
- After validating such a block, proactively tell the user that it is ready to commit and propose a concise Conventional Commit message.
- Before committing, inspect the diff and run the checks relevant to the changed block.
