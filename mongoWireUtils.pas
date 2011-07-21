unit mongoWireUtils;
{
   Misc utils for TMongoWire


   Originally developped by Alexey Petushkov (mentatxx@gmail.com)
   under MIT License
}
interface
uses Windows, Classes, SysUtils, Variants, Types, bsonDoc;

{**
  *  Dump BSON Variant to string
*}
function bsonDump( const V: Variant ) : string;


{**
  * Convert local time to UTC
  * @param DateTime
  * @return
*}
function convertTimeLocalToUTC(DateTime: TDateTime): TDateTime;


{**
  * Convert UTC time to local
  * @param DateTime
  * @return
*}
function convertTimeUTCToLocal(DateTime: TDateTime): TDateTime;

implementation

resourcestring
  RsMakeUTCTime    = 'Error converting from/to UTC time. Time zone could not be determined';

const
  MinutesPerDay     = 60 * 24;  

type
  TIndexArray = TIntegerDynArray;

function ConcatArrays( A1, A2: TIndexArray): TIndexArray;
var
  i: Integer;
begin
  SetLength( Result, High(A1) + High(A2) + 2 );
  for i := 0 to High(A1) do
    Result[i]:= A1[i];

  for i := 0 to High(A2) do
    Result[High(A1)+1+i]:= A2[i];
end;


function joinIndexes( const Indexes:  TIndexArray;
    const Separator: string = ' x ') : string;
var i : integer;
begin
  Result := '';
  if length(Indexes)=0 then exit;
  Result := IntToStr( Indexes[0] );
  for i := 1 to Length(Indexes) - 1 do
    Result := Result + Separator + IntToStr( Indexes[i] );
end;


function bsonDump( const V: Variant ) : string;
  function varDumpRow( const V: Variant; const Dim: Integer; const P: TIndexArray ): string;
  var i, l: integer;
      Z: TIndexArray;
  begin

    Result := '';
    l := length(P);
    for i := VarArrayLowBound(V, 1+length(P))  to VarArrayHighBound(V, 1+length(P)) do
      begin
        SetLength(Z, 1);
        Z[0] := i;
        Z := ConcatArrays(P, Z);
        if l+1=Dim then
          begin
            try
              Result := Result +  joinIndexes(Z) + '  ' + bsonDump( VarArrayGet(V, Z) ) + #13#10
            except on E: Exception do
              Result := Result +  joinIndexes(Z) + '  ERROR '#13#10;
            end;
          end
        else
            Result := Result + varDumpRow( V, Dim, Z ) + #13#10;
      end;

  end;

var d: integer;
    Z: TIndexArray;
    VV: Variant;
begin
  if VarIsArray(V) then
    begin
      SetLength(Z, 0);
      d := VarArrayDimCount( V );
      Result := varDumpRow( V,  d, Z );
    end else
  if varType(V) = 13 then
      begin
        VV := (IUnknown(V) as IBSONDocument).ToVarArray;
        Result := bsonDump( VV );
      end else
      begin
        try
          Result := VarToStr(V);
        except on E: Exception do
          Result := 'Error varType = (' + IntToStr(VarType(V)) + ')';
        end;
      end;
end;

function convertTimeLocalToUTC(DateTime: TDateTime): TDateTime;
var
  TimeZoneInfo: TTimeZoneInformation;
begin
  ZeroMemory(@TimeZoneInfo, SizeOf(TimeZoneInfo));
  case GetTimeZoneInformation(TimeZoneInfo) of
    TIME_ZONE_ID_STANDARD, TIME_ZONE_ID_UNKNOWN:
      Result := DateTime + (TimeZoneInfo.Bias + TimeZoneInfo.StandardBias) / MinutesPerDay;
    TIME_ZONE_ID_DAYLIGHT:
      Result := DateTime + (TimeZoneInfo.Bias + TimeZoneInfo.DaylightBias) / MinutesPerDay;
  else
    raise Exception.CreateRes(@RsMakeUTCTime);
  end;
end;

function convertTimeUTCToLocal(DateTime: TDateTime): TDateTime;
var
  TimeZoneInfo: TTimeZoneInformation;
begin
  ZeroMemory(@TimeZoneInfo, SizeOf(TimeZoneInfo));
  case GetTimeZoneInformation(TimeZoneInfo) of
    TIME_ZONE_ID_STANDARD, TIME_ZONE_ID_UNKNOWN:
      Result := DateTime - (TimeZoneInfo.Bias + TimeZoneInfo.StandardBias) / MinutesPerDay;
    TIME_ZONE_ID_DAYLIGHT:
      Result := DateTime - (TimeZoneInfo.Bias + TimeZoneInfo.DaylightBias) / MinutesPerDay;
  else
    raise Exception.CreateRes(@RsMakeUTCTime);
  end;
end;


end.
