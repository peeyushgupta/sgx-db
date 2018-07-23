#include "db.hpp"
#include <cstdlib>
#include "obli.hpp"
#include <cstdio>
Table g_tbl[MAX_TABLES];
s64 varchar_cmp(u8 *va, u8 *vb, u64 max)
{
  u64 la = *((u64 *)va), lb = *((u64 *)vb);
  s64 oblires = obli_cmp64(la, lb);
  s64 out = obli_varcmp(&va[8], &vb[8], obli_cmov64(la, lb, oblires, 1), max);
  return obli_cmov64(out, oblires, out, 0);
}
s64 str_cmp(u8 *s1, u8 *s2, u64 len)
{
  return obli_strcmp(s1, s2, len);
}
s64 varbinary_cmp(u8 *va, u8 *vb, u64 max)
{
  u64 la = (7+*((u64 *)va))/8, lb = (7+*((u64 *)vb))/8;
  s64 oblires = obli_cmp64(la, lb);
  s64 out = obli_varcmp(&va[8], &vb[8], obli_cmov64(la, lb, oblires, 1), max);
  return obli_cmov64(out, oblires, out, 0);
}
u64 col_len(Column *col)
{
  u64 ret;
  switch (col[0].ty)
  {
    case BOOLEAN:
    {
      ret= 1;
    }
    case BINARY:
    {
      ret= (col[0].buf.len + 7) / 8;
    }
    case VARBINARY:
    {
      ret= 8 + ((col[0].buf.len + 7) / 8);
    }
    case CHARACTER:
    {
      ret= col[0].buf.len;
    }
    case VARCHAR:
    {
      ret= 8 + col[0].buf.len;
    }
    case DECIMAL:
    {
      ret= (col[0].dec.decimal + col[0].dec.integral + 7) / 8;
    }
  }
  return ret;
}
u64 schema_rowlen(Schema *tbl)
{
  u64 cur = 0;
  for (u64 i = 0; i < MAX_COLS; i++)
  {
    s64 res = obli_cmp64(i, tbl->lenRow);
    res -=1;
    res*=-1;
    res= res>>1;
    cur += col_len(&tbl->schema[i])*res;

  }
  return cur;
}
u64 schema_rowoffs(Schema *tbl, u64 num)
{
  u64 cur = 0;
  for (auto i = 0; i < MAX_COLS; i++)
  {
    s64 res = obli_cmp64(i, num);
    res -=1;
    res*=-1;
    res= res>>1;
    cur += col_len(&tbl->schema[i])*res;
  }
  return cur;
}

template <typename T>
void db_tbl_join(void * blkIn, void * blkOut, u64 t1, u64 c1, u64 t2, u64 c2, T cb)
{
  Column col1, col2;
  u64 off1 = 0, off2 = 0;
  u64 len1 = 0, len2 = 0;
  for(u64 i = 0;i<MAX_TABLES;i++){
    Schema * cur = g_tbl[i].sc;
    s64 tind1 = obli_cmp64(t1, i);
    s64 tind2 = obli_cmp64(t2, i);
    for (u64 j = 0; j < MAX_COLS; j++)
    {
      s64 cind1 = obli_cmp64(j, c1), cind2 = obli_cmp64(j, c2);
      s64 eq1 = (cind1*cind1)+(tind1*tind1), eq2 = (cind2*cind2)+(tind2*tind2); 
      obli_cmov(col1,cur->schema[j],eq1, 0);
      obli_cmov(col2,cur->schema[j],eq2, 0);
      
    }
  }
}
