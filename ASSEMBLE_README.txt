Final step: assemble SUBMISSION.md and commit
=============================================

INSTALL (from inside your repo folder)

  tar -xf "%USERPROFILE%\Downloads\nammamart_assemble_bhavani.zip" --strip-components=1

THREE FILES
  include/assemble_submission.py   stitches everything into one SUBMISSION.md
  SUBMISSION_partF.md              Part F, 6 of 10 experiments already filled in
  SUBMISSION_partD.md              Part D, written for you

--------------------------------------------------------------------
STEP 1: make sure all the part files are in the repo root
--------------------------------------------------------------------

  dir SUBMISSION_part*.md

You should see partA, partB, partC, partD, partE, partF, partG.
If any are missing, download them from the earlier zips.
Part 0 is already inside your current SUBMISSION.md and is handled automatically.

--------------------------------------------------------------------
STEP 2: assemble
--------------------------------------------------------------------

  py -3.12 include\assemble_submission.py

It prints each section as it adds it, then the word count.
Expect roughly 11,700 words across 8 sections.

Your existing SUBMISSION.md is preserved as SUBMISSION_part0.md first, so the
Part 0 profiling work is not lost.

Re-runnable. Running it twice does not nest or duplicate anything.

--------------------------------------------------------------------
STEP 3: check it
--------------------------------------------------------------------

Open SUBMISSION.md and confirm the 8 part headings are present and the tables
render. GitHub previews markdown, so you can also just push and look.

--------------------------------------------------------------------
STEP 4: tidy up
--------------------------------------------------------------------

  del SUBMISSION_part*.md

One document is what the marker wants, not nine.

--------------------------------------------------------------------
STEP 5: commit and push
--------------------------------------------------------------------

GitHub Desktop. Summary:

  Complete submission: both pipelines, governance, observability, recommendation

Commit to main, then Push origin.

Before committing, confirm no warehouse file is in the changed files list. If
include/warehouse_bhavani.duckdb appears, add to .gitignore:

  echo include/warehouse_*.duckdb>> .gitignore
  echo *.duckdb.wal>> .gitignore

--------------------------------------------------------------------
STILL OPTIONAL
--------------------------------------------------------------------

Four Part F experiments are marked "(fill in after running)": 6, 7, 9, 10.
Each takes about 5 minutes:

  py -3.12 include\experiments.py schema
  astro dev run dags test nammamart_etl_bhavani
  py -3.12 include\experiments.py restore

Replace "schema" with "volume" then "stale". Experiment 10 needs no scenario,
just click Clear on a failed task in the Airflow UI.

Edit SUBMISSION.md directly afterwards, or edit SUBMISSION_partF.md and re-run
the assembler.

Also still open, your call:
  - negative quantities: errors (current) or refunds
  - screenshots folder: make sure your captures are committed
