Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$encoding = [System.Text.Encoding]::GetEncoding(866)
$utf8 = [System.Text.Encoding]::UTF8
$basePath = 'C:\Users\AVXUser\SC'

# Only keep these fields (PROC_TYPE removed)
$keepFields = @('PROC_ID', 'STDC_DLAB', 'STDC_OVHD', 'STDC_MATL', 'STDC_DEPR', 'STDC_UPKP', 'STDC_FCTR', 'PROC_FCTR')

function Read-AllDbfRecords {
    param([string]$FilePath)
    if (-not (Test-Path $FilePath)) { return $null }
    $fs = [System.IO.File]::OpenRead($FilePath)
    $br = New-Object System.IO.BinaryReader($fs)
    $version = $br.ReadByte()
    $null = $br.ReadBytes(3)
    $numRecords = $br.ReadInt32()
    $headerSize = $br.ReadInt16()
    $recordSize = $br.ReadInt16()
    $null = $br.ReadBytes(20)
    $numFields = [math]::Floor(($headerSize - 33) / 32)
    $fields = @()
    for ($i = 0; $i -lt $numFields; $i++) {
        $nameBytes = $br.ReadBytes(11)
        $rawName = $encoding.GetString($nameBytes).Trim([char]0).Trim()
        $cleanName = ($rawName -replace '[^\x20-\x7E]','').Trim()
        if ($cleanName -eq '') { $cleanName = "FIELD$i" }
        $type = [char]$br.ReadByte()
        $null = $br.ReadBytes(4)
        $length = $br.ReadByte()
        $decimal = $br.ReadByte()
        $null = $br.ReadBytes(14)
        $fields += [PSCustomObject]@{Name=$cleanName; Type=$type; Length=$length; Decimal=$decimal}
    }
    $null = $br.ReadByte()
    $currentPos = $fs.Position
    if ($currentPos -lt $headerSize) { $null = $br.ReadBytes($headerSize - $currentPos) }
    $records = @()
    for ($r = 0; $r -lt $numRecords; $r++) {
        $recBytes = $br.ReadBytes($recordSize)
        if ($recBytes.Length -lt $recordSize) { break }
        if ([char]$recBytes[0] -eq '*') { continue }
        $offset = 1
        $rec = [ordered]@{}
        foreach ($f in $fields) {
            $valBytes = New-Object byte[] $f.Length
            [Array]::Copy($recBytes, $offset, $valBytes, 0, $f.Length)
            $val = $encoding.GetString($valBytes).Trim()
            $rec[$f.Name] = $val
            $offset += $f.Length
        }
        $records += [PSCustomObject]$rec
    }
    $br.Close(); $fs.Close()
    return @{Fields=$fields; Records=$records}
}

function Find-Key {
    param($Record, [string]$Pattern)
    foreach ($prop in $Record.PSObject.Properties) {
        if ($prop.Name -match $Pattern) { return $prop.Name }
    }
    return $null
}

function Escape-Xml {
    param([string]$text)
    if (-not $text) { return '' }
    return $text.Replace('&','&amp;').Replace('<','&lt;').Replace('>','&gt;').Replace('"','&quot;').Replace("'",'&apos;')
}

function Col-Letter {
    param([int]$col)
    $result = ''
    while ($col -gt 0) {
        $col--
        $result = [char](65 + ($col % 26)) + $result
        $col = [math]::Floor($col / 26)
    }
    return $result
}

# ============================================================
# Step 0: Read C_PROC.DBF and build PROC_ID -> PCPROCNO lookup
# ============================================================
Write-Host "Reading C_PROC.DBF..."
$cproc = Read-AllDbfRecords (Join-Path $basePath 'C_PROC.DBF')
$procLookup = @{}
if ($cproc -and $cproc.Records.Count -gt 0) {
    $procIdKey = Find-Key $cproc.Records[0] 'PROC_ID'
    $pcprocnoKey = Find-Key $cproc.Records[0] 'PCPROCNO'
    Write-Host "  PROC_ID key: $procIdKey  PCPROCNO key: $pcprocnoKey"
    foreach ($rec in $cproc.Records) {
        $procId = $rec.$procIdKey
        $pcno = $rec.$pcprocnoKey
        if ($procId -and $pcno) {
            $procLookup[$procId] = $pcno
        }
    }
    Write-Host "  Lookup entries: $($procLookup.Count)"
}

# ============================================================
# Step 1: Read C_WIP
# ============================================================
Write-Host ""
Write-Host "Reading C_WIP.DBF..."
$wip = Read-AllDbfRecords (Join-Path $basePath 'C_WIP.DBF')

$plineMap = [ordered]@{}
foreach ($rec in $wip.Records) {
    $plineKey = Find-Key $rec 'PLINE'
    $wfileKey = Find-Key $rec 'WFILE'
    $descKey = Find-Key $rec 'DESCR'
    $pline = if ($plineKey) { $rec.$plineKey } else { '' }
    $wfile = if ($wfileKey) { $rec.$wfileKey } else { '' }
    $desc = if ($descKey) { $rec.$descKey } else { '' }
    if ($pline -eq '' -or $wfile -eq '') { continue }
    if (-not $plineMap.Contains($pline)) {
        $plineMap[$pline] = @{WFILE=$wfile; DESC=$desc}
    }
}
Write-Host "Unique PLINEs: $($plineMap.Count)"

# ============================================================
# Step 2: Collect sheet data
# ============================================================
$allSheets = [ordered]@{}
$errorFiles = @()

foreach ($entry in $plineMap.GetEnumerator()) {
    $pline = $entry.Key
    $wfile = $entry.Value.WFILE
    $desc = $entry.Value.DESC

    $dbfFile = $null
    $found = Get-ChildItem "$basePath\*.DBF" | Where-Object { $_.BaseName -ieq $wfile } | Select-Object -First 1
    if ($found) { $dbfFile = $found.FullName }

    if (-not $dbfFile) {
        Write-Host "  SKIP $pline - $wfile.DBF not found"
        $errorFiles += "$pline -> $wfile.DBF"
        continue
    }

    Write-Host "  $pline -> $wfile.DBF" -NoNewline
    $data = Read-AllDbfRecords $dbfFile
    if (-not $data -or $data.Records.Count -eq 0) {
        Write-Host " EMPTY"
        continue
    }

    # Filter PROC_TYPE != "A" AND at least one STDC_* field != 0
    $procTypeKey = Find-Key $data.Records[0] 'PROC_TYPE'
    # Find cost STDC_ keys only (exclude STDC_FCTR, STDC_UNIT)
    $stdcKeys = @()
    foreach ($prop in $data.Records[0].PSObject.Properties) {
        if ($prop.Name -match '^STDC_(DLAB|OVHD|MATL|DEPR|UPKP)') { $stdcKeys += $prop.Name }
    }

    $filtered = @()
    foreach ($rec in $data.Records) {
        # Skip PROC_TYPE = "A"
        if ($procTypeKey -and $rec.$procTypeKey -eq 'A') { continue }
        # Check at least one STDC_* != 0
        $hasNonZero = $false
        foreach ($sk in $stdcKeys) {
            $v = $rec.$sk
            if ($v -and $v -ne '' -and $v -ne '0' -and $v -ne '0.000' -and $v -ne '0.0000') {
                $numV = 0.0
                if ([double]::TryParse($v, [System.Globalization.NumberStyles]::Any, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$numV)) {
                    if ($numV -ne 0) { $hasNonZero = $true; break }
                }
            }
        }
        if ($hasNonZero) { $filtered += $rec }
    }

    Write-Host " -> $($filtered.Count)/$($data.Records.Count) records"
    if ($filtered.Count -eq 0) { continue }

    # Filter to only keep selected fields
    $filteredFields = @()
    foreach ($f in $data.Fields) {
        foreach ($kf in $keepFields) {
            if ($f.Name -match $kf) { $filteredFields += $f; break }
        }
    }

    # Find PROC_ID key in records for lookup
    $recProcIdKey = Find-Key $filtered[0] 'PROC_ID'

    # Add PCPROCNO to each record and sort by it
    $enriched = @()
    foreach ($rec in $filtered) {
        $procId = if ($recProcIdKey) { $rec.$recProcIdKey } else { '' }
        $pcno = ''
        if ($procId -and $procLookup.ContainsKey($procId)) {
            $pcno = $procLookup[$procId]
        }
        $rec | Add-Member -NotePropertyName 'PCPROCNO' -NotePropertyValue $pcno -Force
        $enriched += $rec
    }

    # Sort by PCPROCNO (numeric sort)
    $sorted = $enriched | Sort-Object {
        $v = $_.PCPROCNO
        $num = 0
        if ([int]::TryParse($v, [ref]$num)) { $num } else { 99999 }
    }

    # Add PCPROCNO as first field in the list
    $pcprocField = [PSCustomObject]@{Name='PCPROCNO'; Type='C'; Length=4; Decimal=0}
    $finalFields = @($pcprocField) + $filteredFields

    $allSheets[$pline] = @{Fields=$finalFields; Records=$sorted; DESC=$desc; WFILE=$wfile}
}

Write-Host ""
Write-Host "Building XLSX with $($allSheets.Count) sheets..."

# Build XLSX manually via ZIP + XML
$outputFile = Join-Path $basePath 'SC_Export.xlsx'
if (Test-Path $outputFile) { Remove-Item $outputFile -Force }

$ms = New-Object System.IO.MemoryStream
$zip = New-Object System.IO.Compression.ZipArchive($ms, [System.IO.Compression.ZipArchiveMode]::Create, $true)

# [Content_Types].xml
$contentTypes = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
"@
$sheetIdx = 1
foreach ($s in $allSheets.Keys) {
    $contentTypes += "`n  <Override PartName=`"/xl/worksheets/sheet$sheetIdx.xml`" ContentType=`"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml`"/>"
    $sheetIdx++
}
$contentTypes += "`n</Types>"

$entry = $zip.CreateEntry('[Content_Types].xml')
$sw = New-Object System.IO.StreamWriter($entry.Open(), $utf8)
$sw.Write($contentTypes)
$sw.Close()

# _rels/.rels
$relsXml = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
"@
$entry = $zip.CreateEntry('_rels/.rels')
$sw = New-Object System.IO.StreamWriter($entry.Open(), $utf8)
$sw.Write($relsXml)
$sw.Close()

# xl/_rels/workbook.xml.rels
$wbRels = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
"@
$sheetIdx = 1
foreach ($s in $allSheets.Keys) {
    $wbRels += "`n  <Relationship Id=`"rId$sheetIdx`" Type=`"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet`" Target=`"worksheets/sheet$sheetIdx.xml`"/>"
    $sheetIdx++
}
$wbRels += "`n  <Relationship Id=`"rIdStyles`" Type=`"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles`" Target=`"styles.xml`"/>"
$wbRels += "`n  <Relationship Id=`"rIdSS`" Type=`"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings`" Target=`"sharedStrings.xml`"/>"
$wbRels += "`n</Relationships>"

$entry = $zip.CreateEntry('xl/_rels/workbook.xml.rels')
$sw = New-Object System.IO.StreamWriter($entry.Open(), $utf8)
$sw.Write($wbRels)
$sw.Close()

# xl/styles.xml (bold style at index 1)
$stylesXml = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="2">
    <font><sz val="11"/><name val="Calibri"/></font>
    <font><b/><sz val="11"/><name val="Calibri"/></font>
  </fonts>
  <fills count="2">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
  </fills>
  <borders count="1">
    <border><left/><right/><top/><bottom/><diagonal/></border>
  </borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="2">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>
  </cellXfs>
</styleSheet>
"@
$entry = $zip.CreateEntry('xl/styles.xml')
$sw = New-Object System.IO.StreamWriter($entry.Open(), $utf8)
$sw.Write($stylesXml)
$sw.Close()

# Shared strings - collect all unique strings
Write-Host "Building shared strings..."
$ssDict = @{}
$ssList = [System.Collections.ArrayList]::new()

function Get-SSIndex {
    param([string]$str)
    if ($ssDict.ContainsKey($str)) { return $ssDict[$str] }
    $idx = $ssList.Count
    $null = $ssList.Add($str)
    $ssDict[$str] = $idx
    return $idx
}

# Pre-populate shared strings from all sheets
foreach ($sheetEntry in $allSheets.GetEnumerator()) {
    $sd = $sheetEntry.Value
    $pline = $sheetEntry.Key
    $null = Get-SSIndex "PLINE: $pline"
    $null = Get-SSIndex "File: $($sd.WFILE)"
    $null = Get-SSIndex $sd.DESC
    foreach ($f in $sd.Fields) { $null = Get-SSIndex $f.Name }
    foreach ($rec in $sd.Records) {
        foreach ($f in $sd.Fields) {
            $val = $rec.($f.Name)
            if ($f.Type -ne 'N' -and $f.Type -ne 'F') {
                $null = Get-SSIndex $val
            }
        }
    }
}

Write-Host "Shared strings: $($ssList.Count)"

# Write shared strings
$entry = $zip.CreateEntry('xl/sharedStrings.xml')
$sw = New-Object System.IO.StreamWriter($entry.Open(), $utf8)
$sw.Write("<?xml version=`"1.0`" encoding=`"UTF-8`" standalone=`"yes`"?>`n")
$sw.Write("<sst xmlns=`"http://schemas.openxmlformats.org/spreadsheetml/2006/main`" count=`"$($ssList.Count)`" uniqueCount=`"$($ssList.Count)`">")
foreach ($s in $ssList) {
    $sw.Write("<si><t>$(Escape-Xml $s)</t></si>")
}
$sw.Write("</sst>")
$sw.Close()

# xl/workbook.xml
$wbXml = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
"@
$sheetIdx = 1
foreach ($s in $allSheets.Keys) {
    $safeName = Escape-Xml $s
    $wbXml += "    <sheet name=`"$safeName`" sheetId=`"$sheetIdx`" r:id=`"rId$sheetIdx`"/>`n"
    $sheetIdx++
}
$wbXml += "  </sheets>`n</workbook>"

$entry = $zip.CreateEntry('xl/workbook.xml')
$sw = New-Object System.IO.StreamWriter($entry.Open(), $utf8)
$sw.Write($wbXml)
$sw.Close()

# Write each sheet
$sheetIdx = 1
foreach ($sheetEntry in $allSheets.GetEnumerator()) {
    $pline = $sheetEntry.Key
    $sd = $sheetEntry.Value
    Write-Host "  Writing sheet $sheetIdx/$($allSheets.Count): $pline ($($sd.Records.Count) rows)"

    $entry = $zip.CreateEntry("xl/worksheets/sheet$sheetIdx.xml")
    $sw = New-Object System.IO.StreamWriter($entry.Open(), $utf8)
    $sw.Write("<?xml version=`"1.0`" encoding=`"UTF-8`" standalone=`"yes`"?>`n")
    $sw.Write("<worksheet xmlns=`"http://schemas.openxmlformats.org/spreadsheetml/2006/main`">")
    $sw.Write("<sheetData>")

    $numCols = $sd.Fields.Count

    # Row 1: PLINE info (bold)
    $sw.Write("<row r=`"1`">")
    $ssI = Get-SSIndex "PLINE: $pline"
    $sw.Write("<c r=`"A1`" t=`"s`" s=`"1`"><v>$ssI</v></c>")
    $ssI = Get-SSIndex "File: $($sd.WFILE)"
    $sw.Write("<c r=`"B1`" t=`"s`" s=`"1`"><v>$ssI</v></c>")
    $ssI = Get-SSIndex $sd.DESC
    $sw.Write("<c r=`"C1`" t=`"s`" s=`"1`"><v>$ssI</v></c>")
    $sw.Write("</row>")

    # Row 2: Headers (bold)
    $sw.Write("<row r=`"2`">")
    for ($c = 0; $c -lt $numCols; $c++) {
        $colL = Col-Letter ($c+1)
        $ssI = Get-SSIndex $sd.Fields[$c].Name
        $sw.Write("<c r=`"${colL}2`" t=`"s`" s=`"1`"><v>$ssI</v></c>")
    }
    $sw.Write("</row>")

    # Data rows
    $rowNum = 3
    foreach ($rec in $sd.Records) {
        $sw.Write("<row r=`"$rowNum`">")
        for ($c = 0; $c -lt $numCols; $c++) {
            $f = $sd.Fields[$c]
            $colL = Col-Letter ($c+1)
            $val = $rec.($f.Name)
            $cellRef = "${colL}${rowNum}"

            if ($f.Type -eq 'N' -or $f.Type -eq 'F') {
                $numVal = 0.0
                if ($val -and [double]::TryParse($val, [System.Globalization.NumberStyles]::Any, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$numVal)) {
                    $sw.Write("<c r=`"$cellRef`"><v>$($numVal.ToString([System.Globalization.CultureInfo]::InvariantCulture))</v></c>")
                } else {
                    if ($val) {
                        $ssI = Get-SSIndex $val
                        $sw.Write("<c r=`"$cellRef`" t=`"s`"><v>$ssI</v></c>")
                    }
                }
            } else {
                if ($val -ne $null) {
                    $ssI = Get-SSIndex $val
                    $sw.Write("<c r=`"$cellRef`" t=`"s`"><v>$ssI</v></c>")
                }
            }
        }
        $sw.Write("</row>")
        $rowNum++
    }

    $sw.Write("</sheetData></worksheet>")
    $sw.Close()
    $sheetIdx++
}

$zip.Dispose()

# Write to file
$bytes = $ms.ToArray()
$ms.Dispose()
[System.IO.File]::WriteAllBytes($outputFile, $bytes)

Write-Host ""
Write-Host "====================================="
Write-Host "DONE! File: $outputFile"
Write-Host "Size: $([math]::Round((Get-Item $outputFile).Length / 1024, 1)) KB"
Write-Host "Sheets: $($allSheets.Count)"
if ($errorFiles.Count -gt 0) {
    Write-Host ""
    Write-Host "Missing files:"
    foreach ($e in $errorFiles) { Write-Host "  $e" }
}
