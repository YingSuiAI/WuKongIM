---
scope: subtree
summary: Builds the bilingual public documentation site from navigation, MDX content, source-backed contracts, and publication evidence.
---

# docs-site Flow

## Responsibility

This directory owns the public documentation application, bilingual content,
navigation, static output, and the JavaScript/Web example. It does not own the
server or SDK implementation described by those documents.

## Boundaries

- Navigation and publication state come from the local navigation model.
- MDX pages and source-backed contract helpers feed the generated site.
- Example compatibility, documentation quality, and production readiness are
  separate claims; a build alone does not establish all three.

## Main Flows

1. Load the navigation model and language-specific MDX through the content
   source configuration.
2. Generate the navigation plan and render published pages in the bilingual
   application shell.
3. Build static pages and language-isolated search, sitemap, and LLM outputs.
4. The build wrapper checks any supplied golden-path receipt against the
   source and example identities before exposing verified compatibility.

## Invariants and Failure Semantics

- Planned entries remain discoverable without becoming published search or
  sitemap entries.
- Missing or mismatched attestation leaves compatibility unverified.
- Package scripts and source checks define the executable verification path;
  phase documents describe their own narrower publication scope.

## Read First

- [Project and content lifecycle](README.md)
- [Navigation model](lib/navigation.ts)
- [Content source](lib/source.ts)
- [Build and attestation wrapper](scripts/build-site.mjs)
- [Executable scripts](package.json)

## Update Triggers

Update this guide when content loading, navigation, publication states, static
outputs, example boundaries, or attestation handling change.
