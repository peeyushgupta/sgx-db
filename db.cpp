#include "db.hpp"
#include <cstdlib>
#include "obli.hpp"
#include <cstdio>
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
void tbl_loop_join(Table *tbl1, Table *tbl2, u64 attr1, u64 attr2, Table *out)
{
  auto o1 = tbl_rowoffs(tbl1, attr1), o2 = tbl_rowoffs(tbl2, attr2);
  auto s1 = tbl_rowlen(tbl1), s2 = tbl_rowlen(tbl2);
  if (tbl1->tblSchema[attr1].ty == VARCHAR)
  {
    u64 len = tbl1->tblSchema[attr1].buf.len;
    for (u64 i = 0; i < tbl1->lenDat; i++)
    {
      for (u64 j = 0; i < tbl2->lenDat; j++)
      {
        u8 * ptr1 = &((u8*)tbl1->dat)[s1*i+o1], ptr2=&((u8*)tbl1->dat)[s1*i+o1];
        s64 result = varchar_cmp(ptr1, ptr2, len);

      }
    }
  }
}

u64 tbl_len(Table *tbl)
{
  return tbl_rowlen(tbl) * tbl->lenDat;
}