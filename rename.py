"""
rename_papers.py
Renames all downloaded PDFs to clean standard names.
Run from project root: python rename_papers.py
"""

import os
from pathlib import Path

RENAMES = {
    "cardiology": {
        "2023_esh_guidelines_for_the_management_of_arterial2.pdf": "esh_guidelines_2023.pdf",
        "accord_trail.pdf":          "accord_trial.pdf",
        "arrive asprin.pdf":         "arrive_aspirin.pdf",
        "Aspree Asprin.pdf":         "aspree_aspirin.pdf",
        "asprin_meta_analysis.pdf":  "aspirin_meta_analysis.pdf",
        "empreor reduced.pdf":       "emperor_reduced.pdf",
        "JUPITER statin.pdf":        "jupiter_statin.pdf",
        "metamorphin cv review.pdf": "metformin_cv_review.pdf",
        "Paradigm.pdf":              "paradigm_hf.pdf",
        "Sprint Trail.pdf":          "sprint_trial.pdf",
    },
    "diabets": {
        "accord_trail1.pdf":        "accord_trial.pdf",
        "ada standard.pdf":         "ada_standards_2024.pdf",
        "advance_trail.pdf":        "advance_trial.pdf",
        "credence.pdf":             "credence_canagliflozin.pdf",
        "Dapa_hf.pdf":              "dapa_hf.pdf",
        "empareg.pdf":              "empa_reg_outcome.pdf",
        "leader.pdf":               "leader_liraglutide.pdf",
        "step_1 weight loss.pdf":   "step1_weight_loss.pdf",
        "sustain-6 .pdf":           "sustain6_semaglutide.pdf",
        "ukpds_followup.pdf":       "ukpds_followup_2008.pdf",
    },
    "oncology": {
        "cart-t therapy.pdf":       "car_t_therapy.pdf",
        "checkmate 227.pdf":        "checkmate_227.pdf",
        "checkpoint_review.pdf":    "checkpoint_review.pdf",
        "flaura_osimertinib.pdf":   "flaura_osimertinib.pdf",
        "Ipilimumab Melanoma.pdf":  "ipilimumab_melanoma.pdf",
        "key_note 024.pdf":         "keynote_024_nsclc.pdf",
        "keynote 552.pdf":          "keynote_552_breast.pdf",
        "monarch.pdf":              "monarch2_breast.pdf",
    },
}

print("\n" + "="*55)
print("  RENAMING PAPERS TO CLEAN NAMES")
print("="*55)

total_ok  = 0
total_err = 0

for domain, files in RENAMES.items():
    print(f"\n  {domain.upper()}")
    print("  " + "-"*48)
    for old_name, new_name in files.items():
        old_path = Path(f"data/pdfs/{domain}/{old_name}")
        new_path = Path(f"data/pdfs/{domain}/{new_name}")

        if not old_path.exists():
            print(f"  ⚠️  NOT FOUND : {old_name}")
            total_err += 1
            continue

        if new_path.exists():
            print(f"  ⏭️  SKIP      : {new_name} (already exists)")
            total_ok += 1
            continue

        try:
            old_path.rename(new_path)
            print(f"  ✅ {old_name[:35]:35s} → {new_name}")
            total_ok += 1
        except Exception as e:
            print(f"  ❌ ERROR: {old_name} → {e}")
            total_err += 1

print("\n" + "="*55)
print(f"  ✅ Renamed : {total_ok}")
print(f"  ❌ Errors  : {total_err}")
print("="*55)

if total_err == 0:
    print("\n  All files renamed cleanly!")
    print("  Next step → python scripts/ingest_all.py")
else:
    print("\n  Fix errors above then re-run.")
print()