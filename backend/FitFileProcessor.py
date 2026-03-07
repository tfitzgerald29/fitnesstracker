import os
import shutil
import zipfile
from pathlib import Path

import polars as pl
from garmin_fit_sdk import Decoder, Stream


class FitFileProcessor:
    """
    A class to handle FIT file processing pipeline including unzipping,
    processing, and storing data in Parquet format.
    """

    def __init__(self, source_folder=None, processedpath=None, mergedfiles_path=None):
        """
        Initialize the FIT file processor.

        Args:
            source_folder: Path to folder containing .zip files with FIT files
            processedpath: Path where extracted .fit files will be stored
            mergedfiles_path: Path where processed parquet files will be saved
        """
        self.source_folder = source_folder
        self.processedpath = processedpath
        self.mergedfiles_path = mergedfiles_path
        self.inclusion_list = [
            "file_id_mesgs",
            "activity_mesgs",
            "session_mesgs",
            "record_mesgs",
            # "set_mesgs",
        ]

    def unzip_fit_files(self):
        """
        Unzip all .zip files containing .fit files from source folder to processed folder.
        Returns list of newly extracted .fit files.
        """
        print(f"Checking for zip files in: {self.source_folder}")
        print(f"Extracting to: {self.processedpath}")

        # Ensure the processed path exists
        os.makedirs(self.processedpath, exist_ok=True)

        new_fit_files = []

        with os.scandir(self.source_folder) as entries:
            for entry in entries:
                if entry.name.endswith(".zip") and entry.is_file():
                    try:
                        print(f"Unzipping: {entry.name}")
                        with zipfile.ZipFile(entry.path, "r") as zip_ref:
                            # Extract only .fit files
                            for file_info in zip_ref.filelist:
                                if file_info.filename.endswith(".fit"):
                                    # Extract directly to processedpath (flattens directory structure)
                                    fit_filename = os.path.basename(file_info.filename)
                                    target_path = os.path.join(
                                        self.processedpath, fit_filename
                                    )

                                    # Extract and save to target path
                                    with (
                                        zip_ref.open(file_info.filename) as source,
                                        open(target_path, "wb") as target,
                                    ):
                                        shutil.copyfileobj(source, target)

                                    new_fit_files.append(target_path)
                                    print(f"  Extracted: {fit_filename}")

                    except Exception as e:
                        print(f"  Error unzipping {entry.name}: {e}")
                        continue

        print(f"Extracted {len(new_fit_files)} new FIT files to {self.processedpath}")
        return new_fit_files

    def get_processed_files(self):
        """
        Get already processed filenames per parquet file.
        Returns a dict mapping msg_type to a set of processed source filenames.
        """
        processed_files = {msg_type: set() for msg_type in self.inclusion_list}

        print(f"Checking for already processed files in: {self.mergedfiles_path}")

        for msg_type in self.inclusion_list:
            parquet_path = os.path.join(self.mergedfiles_path, f"{msg_type}.parquet")
            if os.path.exists(parquet_path):
                try:
                    df = pl.read_parquet(parquet_path)
                    print(
                        f"  ✓ {msg_type}.parquet loaded: {df.shape[0]} rows, {df.shape[1]} cols"
                    )
                    if "source_file" in df.columns:
                        files = set(df["source_file"].unique().to_list())
                        processed_files[msg_type] = files
                        print(f"    {len(files)} unique source files")
                    else:
                        print(f"    ⚠ No 'source_file' column found")
                except Exception as e:
                    print(f"  ✗ {msg_type}.parquet failed to load: {e}")
            else:
                print(f"  ✗ {msg_type}.parquet not found (will be created)")

        return processed_files

    @staticmethod
    def align_schemas(existing_df, new_df):
        existing_cols = set(existing_df.columns)
        new_cols = set(new_df.columns)

        # Add missing columns to new_df, matching existing dtypes
        for col in existing_cols - new_cols:
            dtype = existing_df[col].dtype
            new_df = new_df.with_columns(pl.lit(None).cast(dtype).alias(col))

        # Add missing columns to existing_df, matching new dtypes
        for col in new_cols - existing_cols:
            dtype = new_df[col].dtype
            existing_df = existing_df.with_columns(pl.lit(None).cast(dtype).alias(col))

        # Cast mismatched dtypes in new_df to match existing (existing wins)
        for col in existing_cols & new_cols:
            if existing_df[col].dtype != new_df[col].dtype:
                # Prefer the non-Null type; if both are non-Null, existing wins
                if new_df[col].dtype == pl.Null:
                    new_df = new_df.with_columns(pl.lit(None).cast(existing_df[col].dtype).alias(col))
                elif existing_df[col].dtype == pl.Null:
                    existing_df = existing_df.with_columns(pl.lit(None).cast(new_df[col].dtype).alias(col))
                else:
                    try:
                        new_df = new_df.with_columns(pl.col(col).cast(existing_df[col].dtype))
                    except Exception:
                        pass  # leave as-is if cast fails, concat will use supertype

        # Sort columns to match order
        all_cols = sorted(existing_cols | new_cols)
        existing_df = existing_df.select(all_cols)
        new_df = new_df.select(all_cols)

        return existing_df, new_df

    def process_new_fit_files(self, new_fit_files, already_processed):
        # Initialize dictionaries to store new data
        data_by_type = {msg_type: [] for msg_type in self.inclusion_list}

        # Track processing results
        new_file_count = 0
        skipped_count = 0
        schema_mismatch_files = []
        processing_error_files = []

        # Process each FIT file from memory
        for file_path in new_fit_files:
            filename = os.path.basename(file_path)

            try:
                new_file_count += 1
                print(f"Processing new file: {filename}")

                stream = Stream.from_file(file_path)
                decoder = Decoder(stream)
                messages, errors = decoder.read()

                if errors:
                    print(f"  Warnings/Errors in {filename}: {len(errors)}")

                # Process each message type, skipping if already in that parquet
                for msg_type in self.inclusion_list:
                    if filename in already_processed[msg_type]:
                        continue
                    if msg_type in messages:
                        for msg in messages[msg_type]:
                            # Filter to only include string keys
                            filtered_msg = {
                                key: value
                                for key, value in msg.items()
                                if isinstance(key, str)
                            }
                            filtered_msg["source_file"] = filename
                            data_by_type[msg_type].append(filtered_msg)

            except Exception as e:
                print(f"  Error processing {filename}: {e}")
                processing_error_files.append({"file": filename, "error": str(e)})
                continue

        print(
            f"\nProcessed {new_file_count} new files, skipped {skipped_count} already processed"
        )

        os.makedirs(self.mergedfiles_path, exist_ok=True)

        if new_file_count > 0:
            print(f"\nUpdating Parquet files in: {self.mergedfiles_path}")
            for msg_type, data in data_by_type.items():
                parquet_path = os.path.join(
                    self.mergedfiles_path, f"{msg_type}.parquet"
                )

                if data:
                    try:
                        new_df = pl.DataFrame(data)

                        if os.path.exists(parquet_path):
                            try:
                                existing_df = pl.read_parquet(parquet_path)
                                existing_df, new_df = self.align_schemas(
                                    existing_df, new_df
                                )
                                combined_df = pl.concat(
                                    [existing_df, new_df], how="diagonal_relaxed"
                                )
                                combined_df.write_parquet(parquet_path)
                                print(
                                    f"✓ {msg_type}.parquet: Added {new_df.shape[0]} rows (total: {combined_df.shape[0]})"
                                )
                            except Exception as e:
                                print(
                                    f"  ✗ Failed to merge {msg_type}: {e}"
                                )
                                failed_files = (
                                    new_df["source_file"].unique().to_list()
                                )
                                schema_mismatch_files.extend(
                                    [
                                        {
                                            "file": f,
                                            "msg_type": msg_type,
                                            "error": str(e),
                                        }
                                        for f in failed_files
                                    ]
                                )
                        else:
                            # Create new file
                            new_df.write_parquet(parquet_path)
                            print(
                                f"✓ {msg_type}.parquet: Created with {new_df.shape[0]} rows"
                            )

                    except Exception as e:
                        print(f"  ✗ Error creating DataFrame for {msg_type}: {e}")
                        continue
                else:
                    if not os.path.exists(parquet_path):
                        print(f"✗ {msg_type}: No data found")
        else:
            print("\nNo new files to process.")

        # Print summary of issues
        if schema_mismatch_files or processing_error_files:
            print("\n" + "=" * 60)
            print("PROCESSING SUMMARY")
            print("=" * 60)

            if processing_error_files:
                print(
                    f"\n⚠ Files with processing errors ({len(processing_error_files)}):"
                )
                for item in processing_error_files:
                    print(f"  - {item['file']}: {item['error']}")

            if schema_mismatch_files:
                print(
                    f"\n⚠ Files with schema mismatches ({len(schema_mismatch_files)}):"
                )
                for item in schema_mismatch_files:
                    print(f"  - {item['file']} ({item['msg_type']}): {item['error']}")

        print("\nDone!")

        return {
            "new_files_processed": new_file_count,
            "skipped_already_processed": skipped_count,
            "schema_mismatch_files": schema_mismatch_files,
            "processing_error_files": processing_error_files,
        }

    def run(self):
        print("=" * 60)
        print("Starting Incremental FIT File Processing Pipeline")
        print("=" * 60)

        # Ensure directories exist
        os.makedirs(self.processedpath, exist_ok=True)
        os.makedirs(self.mergedfiles_path, exist_ok=True)

        # Step 1: Unzip new files to processedpath (returns list of files)
        print("\n[Step 1] Unzipping files...")
        new_fit_files = self.unzip_fit_files()

        # Step 2: Get already processed files
        print("\n[Step 2] Checking for already processed files...")
        already_processed = self.get_processed_files()

        # Step 3: Process new files using in-memory lists
        print("\n[Step 3] Processing FIT files...")
        summary = self.process_new_fit_files(new_fit_files, already_processed)

        print("\n" + "=" * 60)
        print("Pipeline Complete!")
        print("=" * 60)
        print(f"New files processed: {summary['new_files_processed']}")
        print(
            f"Files skipped (already processed): {summary['skipped_already_processed']}"
        )
        print(f"Files with schema mismatches: {len(summary['schema_mismatch_files'])}")
        print(f"Files with processing errors: {len(summary['processing_error_files'])}")

        return summary

    def rebuild(self):
        """Delete merged parquets and re-process all FIT files from processedfiles."""
        print("=" * 60)
        print("Rebuilding all merged parquet files from scratch")
        print("=" * 60)

        # Remove existing merged parquets
        for msg_type in self.inclusion_list:
            parquet_path = os.path.join(self.mergedfiles_path, f"{msg_type}.parquet")
            if os.path.exists(parquet_path):
                os.remove(parquet_path)
                print(f"  Removed {msg_type}.parquet")

        # Get all FIT files from processedfiles
        all_fit_files = [
            os.path.join(self.processedpath, f)
            for f in os.listdir(self.processedpath)
            if f.endswith(".fit")
        ]
        print(f"\nFound {len(all_fit_files)} FIT files to re-process")

        # Process with empty already_processed set
        empty_processed = {msg_type: set() for msg_type in self.inclusion_list}
        summary = self.process_new_fit_files(all_fit_files, empty_processed)

        print("\n" + "=" * 60)
        print("Rebuild Complete!")
        print("=" * 60)
        print(f"Files processed: {summary['new_files_processed']}")
        print(f"Files with schema mismatches: {len(summary['schema_mismatch_files'])}")
        print(f"Files with processing errors: {len(summary['processing_error_files'])}")

        return summary
