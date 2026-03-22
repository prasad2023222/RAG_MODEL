from pathlib import Path

EXPECTED = {
    "cardiology": [
        "2023_esh_guidelines_for_the_management_of_arterial2.pdf",  # 24,607 KB
        "accord_trail.pdf",                             # 295 KB
        "arrive asprin.pdf",                            # 1,334 KB
        "Aspree Asprin.pdf",                            # 902 KB
        "asprin_meta_analysis.pdf",                     # 1,237 KB
        "empreor reduced.pdf",                          # 681 KB
        "JUPITER statin.pdf",                           # 363 KB
        "metamorphin cv review.pdf",                    # 3,840 KB
        "Paradigm.pdf",                                 # 646 KB
        "Sprint Trail.pdf",                             # 709 KB
    ],
    "diabets": [
        "accord_trail1.pdf",                            # 295 KB
        "ada standard.pdf",                             # 561 KB
        "advance_trail.pdf",                            # 797 KB
        "credence.pdf",                                 # 455 KB
        "Dapa_hf.pdf",                                  # 472 KB
        "empareg.pdf",                                  # 403 KB
        "leader.pdf",                                   # 349 KB
        "step_1 weight loss.pdf",                       # 540 KB
        "sustain-6 .pdf",                               # 353 KB
        "ukpds_followup.pdf",                           # 555 KB
    ],
    "oncology": [
        "cart-t therapy.pdf",                           # 331 KB
        "checkmate 227.pdf",                            # 386 KB
        "checkpoint_review.pdf",                        # 491 KB
        "flaura_osimertinib.pdf",                       # 545 KB
        "Ipilimumab Melanoma.pdf",                      # 331 KB
        "key_note 024.pdf",                             # 256 KB
        "keynote 552.pdf",                              # 565 KB
        "monarch.pdf",                                  # 1,032 KB
    ],
}

print("\n" + "="*60)
print("  PDF DOWNLOAD VERIFICATION")
print("="*60)

total_ok      = 0
total_missing = 0
total_small   = 0

for domain, files in EXPECTED.items():
    count = len(files)
    print(f"\n  {domain.upper()} ({count} papers)")
    print("  " + "-"*52)

    for fname in files:
        path = Path(f"data/pdfs/{domain}/{fname}")
        if path.exists():
            size_kb = path.stat().st_size // 1024
            if size_kb > 100:
                print(f"  ✅ {fname:48s} {size_kb:6d} KB")
                total_ok += 1
            else:
                print(f"  ⚠️  {fname:48s} {size_kb:6d} KB  TOO SMALL")
                total_small += 1
        else:
            print(f"  ❌ {fname:48s} MISSING")
            total_missing += 1

print("\n" + "="*60)
print(f"  ✅ OK        : {total_ok}")
print(f"  ⚠️  Too small : {total_small}")
print(f"  ❌ Missing   : {total_missing}")
print(f"  📦 Total     : {total_ok + total_small + total_missing}")
print("="*60)

if total_missing == 0 and total_small == 0:
    print("\n  ALL PAPERS READY!")
    print("  Next step → run: python scripts/ingest_all.py")
else:
    print("\n  Fix issues above before ingesting.")
print()