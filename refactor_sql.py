"""
SQL Refactor Script
Tự động refactor code SQL:
1. Comment dblink lại (ví dụ: @fcpdb -> /*@fcpdb*/)
2. Thay tên schema với db link tương ứng
"""

import re
import os
import sys
from pathlib import Path

# Mapping dblink -> schema (không phân biệt hoa thường)
DBLINK_SCHEMA_MAP = {
    "@fcpdb.coreproddb.coreprodvcn.oraclevcn.com": "hoc_tap_fcc_new",
    "@fcpdb": "hoc_tap_fcc",
}

def refactor_sql(content: str) -> str:
    """
    Refactor SQL content:
    1. Backup code gốc vào comment cuối dòng: /* schema.table@dblink */
    2. Thay thế schema mới tương ứng với dblink
    3. Ignore content within existing block comments /* ... */
    """
    
    # Split content into segments: (comment_block, non_comment_block)
    pattern_comment_block = re.compile(r'(/\*[\s\S]*?\*/)')
    segments = pattern_comment_block.split(content)
    
    # Sắp xếp dblink theo độ dài giảm dần để match dblink dài trước
    sorted_dblinks = sorted(DBLINK_SCHEMA_MAP.keys(), key=len, reverse=True)
    ident = r'[a-zA-Z_][a-zA-Z0-9_$#]*'
    
    processed_segments = []
    
    for i, segment in enumerate(segments):
        # If segment starts with /*, it's a comment block - skip processing
        if segment.startswith('/*') and segment.endswith('*/'):
            processed_segments.append(segment)
            continue
            
        # Refactor logic for non-comment code
        # Process line by line để thêm comment backup vào cuối dòng
        lines = segment.split('\n')
        processed_lines = []
        
        for line in lines:
            current_line = line
            backups = []
            
            for dblink in sorted_dblinks:
                new_schema = DBLINK_SCHEMA_MAP[dblink]
                dblink_name = dblink[1:] if dblink.startswith('@') else dblink
                
                # Regex: (schema.)?(table)@dblink_name
                pattern = re.compile(
                    r'((' + ident + r')\.)?(' + ident + r')@' + re.escape(dblink_name) + r'\b',
                    re.IGNORECASE
                )
                
                # Tìm tất cả match trên dòng này để lưu backup
                matches = list(pattern.finditer(current_line))
                if matches:
                    # Lưu các cụm dblink gốc để cho vào comment backup
                    for m in matches:
                        if m.group(0) not in backups:
                            backups.append(m.group(0))
                    
                    # Thay thế từ phải sang trái để không bị sai vị trí
                    for match in reversed(matches):
                        table_name = match.group(3)
                        # Tạo code mới
                        new_code = f"{new_schema}.{table_name}"
                        # Thay thế trong dòng
                        current_line = current_line[:match.start()] + new_code + current_line[match.end():]
            
            # Nếu có thay đổi, thêm comment backup chứa các cụm dblink gốc vào cuối dòng
            if backups:
                backup_content = " ".join(backups)
                # Xóa khoảng trắng cuối dòng trước khi append comment
                current_line = f"{current_line.rstrip()} /* {backup_content} */"
            
            processed_lines.append(current_line)
        
        processed_segments.append('\n'.join(processed_lines))
    
    return "".join(processed_segments)


def process_file(file_path: str, output_path: str = None, dry_run: bool = False) -> bool:
    """
    Process a single SQL file
    
    Args:
        file_path: Path to input SQL file
        output_path: Path to output file (None = overwrite input)
        dry_run: If True, only print changes without writing
    
    Returns:
        True if file was modified, False otherwise
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            original_content = f.read()
    
    refactored_content = refactor_sql(original_content)
    
    if original_content == refactored_content:
        print(f"[SKIP] {file_path} - Không có thay đổi")
        return False
    
    if dry_run:
        print(f"[DRY-RUN] {file_path} - Sẽ được cập nhật")
        return True
    
    output = output_path or file_path
    with open(output, 'w', encoding='utf-8') as f:
        f.write(refactored_content)
    
    print(f"[OK] {file_path} -> {output}")
    return True


def process_directory(dir_path: str, extensions: list = None, dry_run: bool = False, recursive: bool = True) -> tuple:
    """
    Process all SQL files in a directory
    
    Args:
        dir_path: Path to directory
        extensions: List of file extensions to process (default: ['.sql', '.txt'])
        dry_run: If True, only print changes without writing
        recursive: If True, process subdirectories
    
    Returns:
        Tuple of (processed_count, modified_count)
    """
    if extensions is None:
        extensions = ['.sql', '.txt']
    
    processed = 0
    modified = 0
    
    path = Path(dir_path)
    
    if recursive:
        files = path.rglob('*')
    else:
        files = path.glob('*')
    
    for file_path in files:
        if file_path.is_file() and file_path.suffix.lower() in extensions:
            processed += 1
            if process_file(str(file_path), dry_run=dry_run):
                modified += 1
    
    return processed, modified


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='SQL Refactor Script - Comment dblink và thay thế schema',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Ví dụ sử dụng:
  python refactor_sql.py file.sql                    # Refactor 1 file
  python refactor_sql.py folder/                     # Refactor tất cả file trong folder
  python refactor_sql.py folder/ --dry-run           # Xem trước thay đổi
  python refactor_sql.py folder/ --ext .sql .txt     # Chỉ xử lý file .sql và .txt
  python refactor_sql.py folder/ --no-recursive      # Không xử lý folder con
        '''
    )
    
    parser.add_argument('path', help='Đường dẫn file hoặc folder cần refactor')
    parser.add_argument('--dry-run', '-n', action='store_true', 
                        help='Chỉ hiển thị thay đổi, không ghi file')
    parser.add_argument('--ext', nargs='+', default=['.sql', '.txt'],
                        help='Danh sách extension cần xử lý (mặc định: .sql .txt)')
    parser.add_argument('--no-recursive', '-nr', action='store_true',
                        help='Không xử lý folder con')
    parser.add_argument('--output', '-o', help='Đường dẫn output (chỉ dùng khi input là 1 file)')
    
    args = parser.parse_args()
    
    path = Path(args.path)
    
    if not path.exists():
        print(f"Lỗi: Không tìm thấy '{args.path}'")
        sys.exit(1)
    
    if path.is_file():
        success = process_file(str(path), args.output, args.dry_run)
        sys.exit(0 if success else 1)
    elif path.is_dir():
        processed, modified = process_directory(
            str(path), 
            extensions=args.ext,
            dry_run=args.dry_run,
            recursive=not args.no_recursive
        )
        print(f"\nTổng kết: {modified}/{processed} file được cập nhật")
        sys.exit(0)
    else:
        print(f"Lỗi: '{args.path}' không phải file hoặc folder")
        sys.exit(1)


if __name__ == '__main__':
    main()
