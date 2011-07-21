unit mongoWireGridFS;
{  
   GridFS support for TMongoWire


   Originally developped by Alexey Petushkov (mentatxx@gmail.com)
   under MIT License
}
interface
uses Windows, Classes, SysUtils, Variants, Types,
     bsonDoc, mongoWire, mongoWireUtils;

  type
  TMongoGridFile = class( TStream )
    private
      FWire: TMongoWire;
      FChunkSize: DWord;
      FNewFile: boolean;
      FFileName: string;
      FMimeType: string;
      FFileLength: Int64;
      FCreateDate: TDateTime;
      FOffset: Int64;
      FDatabaseName: String;
      FPrefix: String;
      // evaluate MD5 sum for file
      function evalMD5Sum: string;
    private
      Buffer: Variant;
      CurrentPage: Integer;
      fileId: Variant;
      FMetadata: IBSONDocument;
      //
      procedure ReadPage( const Page: Integer );
      procedure WritePage( const Page: Integer; const Size: DWord );
    public
      constructor Create( const Wire: TMongoWire;
                          const Database: string;
                          const FileName: string;
                          const Prefix: string='fs';
                          const DefaultChunkSize: DWord = 256*1024;
                          const MimeType: string = 'application/octet-stream';
                          const MetaData: IBSONDocument = nil
                         );
      destructor Destroy; override;

      // Save info - length & evaluate md5 checksum
      procedure Flush;
    public
      // TStream overrides
      function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
      function Read(var Buffer; Count: Longint): Longint;  override;
      function Write(const Buffer; Count: Longint): Longint;  override;
    public
      class function FileExists( const FileName: string; const Wire: TMongoWire ): boolean;
      class function GetVarField( const O: OleVariant; const FieldName: string; var Value: DWORD ): boolean; overload;
      class function GetVarField( const O: OleVariant; const FieldName: string; var Value: Int64 ): boolean; overload;
      class function GetVarField( const O: OleVariant; const FieldName: string; var Value: String ): boolean; overload;
      class function GetVarField( const O: OleVariant; const FieldName: string; var Value: TDateTime ): boolean; overload;
      class function GetVarField( const O: OleVariant; const FieldName: string; var Value: Variant ): boolean; overload;
      property Wire: TMongoWire read FWire;
      property ChunkSize: DWord read FChunkSize;
      property NewFile: boolean read FNewFile;
      property MimeType: string read FMimeType;
      property FileName: string read FFileName;
      property FileLength: Int64 read FFileLength;
      property CreateDate: TDateTime read FCreateDate;
      property Prefix: String read FPrefix;
      property DatabaseName: String read FDatabaseName;
      property ObjectId: Variant read fileId;
  end;


implementation

{ TMongoGridFile }

constructor TMongoGridFile.Create( const Wire: TMongoWire;
                                   const Database: string;
                                   const FileName: string;
                                   const Prefix: string='fs';
                                   const DefaultChunkSize: DWord = 256*1024;
                                   const MimeType: string = 'application/octet-stream';
                                   const MetaData: IBSONDocument = nil );
var Q: TMongoWireQuery;
    O: IBSONDocument;
    V: Variant;
    fc: string;
begin
  //
  FWire := Wire;
  FPrefix := Prefix;
  FDatabaseName := Database;
  FFileName := FileName;
  FMimeType := MimeType;
  FMetadata := MetaData;
  Q := TMongoWireQuery.Create(Wire);
  try
  O := BSON( ['filename', FileName] );
  fc := DatabaseName+'.'+Prefix+ '.files';
  Q.Query(fc, O );
  if Q.Next(O) then
    begin
       V := O.ToVarArray;
       FNewFile := False;
       Assert( GetVarField(V, '_id', fileId ) );
       Assert( GetVarField(V, 'chunkSize', FChunkSize ) );
       Assert( GetVarField(V, 'length', FFileLength) );
       Assert( GetVarField(V, 'uploadDate', FCreateDate) );
       if not GetVarField(V, 'contentType', FMimeType) then
           FMimeType := MimeType;
    end else
    begin
       //
       FNewFile := True;
       FChunkSize := DefaultChunkSize;
       FFileLength := 0;
       FCreateDate := convertTimeLocalToUTC(Now);
       Flush;
    end;
    FOffset := 0;
    // Fill da buffer
    Buffer := VarArrayCreate([0, FChunkSize-1], varByte);
    CurrentPage := -1;
  finally
    FreeAndNil(Q);
  end;
end;

destructor TMongoGridFile.Destroy;
begin
  Flush;
  inherited;
end;

function TMongoGridFile.evalMD5Sum: string;
begin
  { TODO :  Just do it}
  Result := '';
end;

class function TMongoGridFile.FileExists(const FileName: string;
  const Wire: TMongoWire): boolean;
begin
   { TODO : Just do it }
   Raise Exception.Create('TMongoGridFile.FileExists not implemented');
end;

procedure TMongoGridFile.Flush;
var Selector: IBSONDocument;
    Doc: IBSONDocument;
    md5sum: string;
begin
  Selector := BSON( ['filename', FileName ] );
  md5sum := evalMD5Sum;
  if NewFile then
  begin
    if Assigned(FMetadata) then
    Doc :=  BSON( [
      'filename', FileName,
      'chunkSize', ChunkSize,
      'uploadDate', CreateDate,
      'contentType', MimeType,
      'length', FileLength,
      'md5', md5sum,
      'metadata', FMetadata
       ] )
    else
    Doc :=  BSON( [
      'filename', FileName,
      'chunkSize', ChunkSize,
      'uploadDate', CreateDate,
      'contentType', MimeType,
      'length', FileLength,
      'md5', md5sum
       ] );
  end
  else
    Doc :=  BSON( [ '$set', '[',
      'length', FileLength,
      'md5', md5sum,
      ']'
      ] );
  // make an upsert
  Wire.Update(DatabaseName+'.'+Prefix+ '.files', Selector, Doc, True);
  repeat
    Doc := Wire.Get(DatabaseName+'.'+Prefix+ '.files', Selector);
    fileId := Doc['_id'];
  until not VarIsNull( fileId );
end;

class function TMongoGridFile.GetVarField(const O: OleVariant;
  const FieldName: string; var Value: TDateTime): boolean;
var i: integer;
begin
  Result := False;
  Assert( VarArrayDimCount(O)=2 );

  i := VarArrayLowBound(O, 1);
  while i <= VarArrayHighBound(O, 1) do
    begin
      if VarArrayGet( O, [i, 0] ) = FieldName  then
        begin
          Value := VarArrayGet( O, [i,1] );
          Result := True;
          break;
        end;
       i := i+1;
    end;
end;

class function TMongoGridFile.GetVarField(const O: OleVariant;
  const FieldName: string; var Value: Int64): boolean;
var i: integer;
begin
  Result := False;
  Assert( VarArrayDimCount(O)=2 );

  i := VarArrayLowBound(O, 1);
  while i <= VarArrayHighBound(O, 1) do
    begin
      if VarArrayGet( O, [i, 0] ) = FieldName  then
        begin
          Value := VarArrayGet( O, [i,1] );
          Result := True;
          break;
        end;
       i := i+1;
    end;
end;

class function TMongoGridFile.GetVarField(const O: OleVariant;
  const FieldName: string; var Value: String): boolean;
var i: integer;
begin
  Result := False;
  Assert( VarArrayDimCount(O)=2 );

  i := VarArrayLowBound(O, 1);
  while i <= VarArrayHighBound(O, 1) do
    begin
      if VarArrayGet( O, [i, 0] ) = FieldName  then
        begin
          Value := VarArrayGet( O, [i,1] );
          Result := True;
          break;
        end;
       i := i+1;
    end;
end;

class function TMongoGridFile.GetVarField(const O: OleVariant;
  const FieldName: string; var Value: DWORD): boolean;
var i: integer;
begin
  Result := False;
  Assert( VarArrayDimCount(O)=2 );

  i := VarArrayLowBound(O, 1);
  while i <= VarArrayHighBound(O, 1) do
    begin
      if VarArrayGet( O, [i, 0] ) = FieldName  then
        begin
          Value := VarArrayGet( O, [i,1] );
          Result := True;
          break;
        end;
       i := i+1;
    end;
end;

class function TMongoGridFile.GetVarField(const O: OleVariant;
  const FieldName: string; var Value: Variant): boolean;
var i: integer;
begin
  Result := False;
  Assert( VarArrayDimCount(O)=2 );

  i := VarArrayLowBound(O, 1);
  while i <= VarArrayHighBound(O, 1) do
    begin
      if VarArrayGet( O, [i, 0] ) = FieldName  then
        begin
          Value := VarArrayGet( O, [i,1] );
          Result := True;
          break;
        end;
       i := i+1;
    end;
end;


function TMongoGridFile.Read(var Buffer; Count: Integer): Longint;
var Left: Int64;
    Page: Integer;
    Len, Ofs: Cardinal;
    Target: Pointer;
    ReadBuf, ReadBufStart: Pointer;
begin
  Left := Count;
  Target := @Buffer;
  Page := FOffset div ChunkSize;
  Ofs := FOffset mod ChunkSize;
  Len := ChunkSize - Ofs;
  if Len>Left then
    Len := Left;
  //

  while Left>0 do
    begin
      // read from db
      ReadPage( Page );
      // копирование данных
      ReadBufStart := VarArrayLock(Self.Buffer);
      try
        ReadBuf := ReadBufStart;
        Inc( pchar(ReadBuf), Ofs );
        Move( ReadBuf^, Target^, Len);
        // уменьшаем оставшееся на количество скачанного
        Left :=  Left - Len;
        // смещаем указатель в целевом буфере
        inc( pchar(Target), Len );
        FOffset := FOffset + Len;
        // продолжаем сначала страницы
        Ofs := 0;
        // предполагаем максимальную длину до чанка
        Len := ChunkSize;
        if Len>Left then
           Len := Left;
        inc(Page); // next Page
      finally
        VarArrayUnlock(Self.Buffer);
      end;
    end;
  Result := Count;
end;

procedure TMongoGridFile.ReadPage(const Page: Integer);
var Q: TMongoWireQuery;
    Res: IBSONDocument;
    V, data: Variant;
    M: pointer;
    len: Cardinal;
    p1, p2: pointer;

begin
  if CurrentPage<>Page then
    begin
      CurrentPage := Page;

      Q:= TMongoWireQuery.Create(Wire);
      try
        len := VarArrayHighBound(Buffer, 1) - VarArrayLowBound(Buffer, 1) + 1;
        Assert( len = ChunkSize );
        M := VarArrayLock(Buffer);
        FillChar( M^, ChunkSize, 0 );
        VarArrayUnlock(Buffer);

        Res := BSON(['files_id', fileId, 'n', Page]);
        Q.Query( DatabaseName+'.'+Prefix+ '.chunks', Res {,BSON(['data', 1]) } );
        if Q.Next(Res) then
          begin
            V := Res.ToVarArray;

            Assert( GetVarField( V, 'data', data ) );
            len := VarArrayHighBound(data, 1) - VarArrayLowBound(data, 1) + 1;
            Assert(len<=ChunkSize);
            // copy data
            p1 := VarArrayLock( data );
            p2 := VarArrayLock( Buffer );
            try
              MoveMemory(p2, p1, len);
            finally
              VarArrayUnlock(data);
              VarArrayUnlock(Buffer);
            end;
          end;

      finally
        FreeAndNil(Q);
      end;
    end;
end;

function TMongoGridFile.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  case Origin of
    soBeginning: FOffset := Offset;
    soCurrent: FOffset := FOffset + Offset;
    soEnd: FOffset := FileLength - Offset;
  end;
  Result := FOffset;
end;

function TMongoGridFile.Write(const Buffer; Count: Integer): Longint;
var Left: Int64;
    Page: Integer;
    AOfs, ALen, Len, Ofs: Cardinal;
    Target: Pointer;
    WriteBuf, WriteBufStart: Pointer;
    LastPage: boolean;
begin
  Left := Count;
  Target := @Buffer;
  Page := FOffset div ChunkSize;
  Ofs := FOffset mod ChunkSize;
  Len := ChunkSize - Ofs;
  if Len>Left then
    Len := Left;
  //

  while Left>0 do
    begin
      // читаем данные
      ReadPage( Page );
      // копирование данных
      WriteBufStart := VarArrayLock(Self.Buffer);
      try
        // Сохраняем смещение и длину внутри начала записи 
        ALen := Len;
        AOfs := Ofs;
        // Пишем в страницу данные
        WriteBuf := WriteBufStart;
        Inc( pchar(WriteBuf), Ofs );
        Move( Target^, WriteBuf^, Len);
        // уменьшаем оставшееся на количество скачанного
        Left :=  Left - Len;
        // продолжаем сначала страницы
        Ofs := 0;
        // смещаем указатель в целевом буфере
        inc( pchar(Target), Len );
        FOffset := FOffset + Len;
        // предполагаем максимальную длину до чанка
        Len := ChunkSize;
        if Len>Left then
           Len := Left;
        // проверяем - последняя страница ?
        LastPage := Page >= (FFileLength div FChunkSize);
        if LastPage then  WritePage( Page, ALen + AOfs )
                    else  WritePage( Page, FChunkSize );

        inc(Page); // next Page
      finally
        VarArrayUnlock(Self.Buffer);
      end;
    end;
  if FOffset > FFileLength then
    FFileLength := FOffset;
  
  Result := Count;
end;

procedure TMongoGridFile.WritePage(const Page: Integer; const Size: DWord);
var NewData, Selector: IBSONDocument;
    data: Variant;
    p1, p2: pointer;
begin
        CurrentPage := Page;

        if Size < FChunkSize then
          begin
            // Write only part of chunk
            data := VarArrayCreate( [1, Size], varByte );
            try
              p1 := VarArrayLock( data );
              p2 := VarArrayLock( Buffer );
              Move( p2^, p1^, Size );
            finally
              VarArrayUnlock( data );
              VarArrayUnlock( Buffer );
            end;
          end else
            // Write Entire page
            data := Buffer;

        NewData  := BSON(['files_id', fileId, 'n', Page, 'data', data ]);
        Selector := BSON(['files_id', fileId, 'n', Page]);

        Wire.Update( DatabaseName+'.'+Prefix+ '.chunks', Selector, NewData, True );
end;

end.
