import re
import os

def process_file(file_path):
    print(f"Processing {file_path}...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        return

    new_lines = []
    modified = False
    # Regex to match Javadoc lines with ; //
    # Group 1: Whitespace and * and content before ;
    # Group 2: Content after //
    pattern = re.compile(r'^(\s*\*.*?);\s+//(.*)$')
    
    for line in lines:
        # Handle potential CRLF by stripping only trailing newline
        stripped_line = line.rstrip('\r\n')
        
        match = pattern.match(stripped_line)
        if match:
            part1 = match.group(1)
            part2 = match.group(2)
            # Enforce exactly two spaces
            new_content = f"{part1};  //{part2}"
            
            # Restore the newline characters from the original line
            if line.endswith('\r\n'):
                new_line = new_content + '\r\n'
            elif line.endswith('\n'):
                new_line = new_content + '\n'
            else:
                new_line = new_content # End of file without newline
            
            if new_line != line:
                new_lines.append(new_line)
                modified = True
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
            
    if modified:
        print(f"Modifying {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
    else:
        print(f"No changes for {file_path}")

files = [
    "src/main/java/com/landawn/abacus/util/MemcachedLock.java",
    "src/main/java/com/landawn/abacus/cache/SpyMemcached.java",
    "src/main/java/com/landawn/abacus/cache/OffHeapCacheStats.java",
    "src/main/java/com/landawn/abacus/cache/OffHeapCache25.java",
    "src/main/java/com/landawn/abacus/cache/OffHeapCache.java",
    "src/main/java/com/landawn/abacus/cache/LocalCache.java",
    "src/main/java/com/landawn/abacus/cache/KryoTranscoder.java",
    "src/main/java/com/landawn/abacus/cache/Ehcache.java",
    "src/main/java/com/landawn/abacus/cache/JRedis.java",
    "src/main/java/com/landawn/abacus/cache/DistributedCacheClient.java",
    "src/main/java/com/landawn/abacus/cache/DistributedCache.java",
    "src/main/java/com/landawn/abacus/cache/CacheStats.java",
    "src/main/java/com/landawn/abacus/cache/CaffeineCache.java",
    "src/main/java/com/landawn/abacus/cache/CacheFactory.java",
    "src/main/java/com/landawn/abacus/cache/Cache.java",
    "src/main/java/com/landawn/abacus/cache/AbstractOffHeapCache.java",
    "src/main/java/com/landawn/abacus/cache/AbstractDistributedCacheClient.java",
    "src/main/java/com/landawn/abacus/cache/AbstractCache.java"
]

for file in files:
    process_file(file)
