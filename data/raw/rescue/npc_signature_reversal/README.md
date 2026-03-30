# NPC Signature-Reversal Rescue Raw Sources

This directory stages the raw rescue inputs for `scz_npc_signature_reversal_rescue_task`.

Files:
- `cos_hispc_npc_differential_expression_2017.csv`: extracted NPC differential-expression rows from Hoffman et al. 2017 Supplementary Data 8.
- `sz_npc_drug_gene_perturbations_2018.csv`: extracted `SZ_iPSC_NPC` perturbation rows from Readhead et al. 2018 Supplementary Data 2.
- `clozapine_rescue_gene_labels_2022.csv`: curated post-cutoff positive rescue genes explicitly named in Hribkova et al. 2022.
- `source_manifest.json`: provenance, licenses, upstream URLs, and sha256 digests for both upstream artifacts and checked-in materializations.

Licensing:
- Hoffman et al. 2017 and Readhead et al. 2018 are distributed under CC BY 4.0.
- Hribkova et al. 2022 is distributed under the Frontiers CC BY license.

Leakage boundary:
- Only the 2017 and 2018 raw sources feed the pre-cutoff ranking inputs.
- The 2022 labels are reserved for evaluation and must not be used for feature construction or threshold selection.
