# Built-in Parameter

## Basic Built-in Parameter

| Variable | Declaration Method | Meaning |
| ---- | ---- | -----------------------------| 
| system.biz.date | `${system.biz.date}` | The day before the schedule time of the daily scheduling instance, the format is `yyyyMMdd` |
| system.biz.curdate | `${system.biz.curdate}` | The schedule time of the daily scheduling instance, the format is `yyyyMMdd` |
| system.datetime | `${system.datetime}` | The schedule time of the daily scheduling instance, the format is `yyyyMMddHHmmss` |

## Extended Built-in Parameter

- Support custom variables in the code, declaration way: `${variable name}`. Refers to "System Parameter".

- Benchmark variable defines as `$[...]` format, time format `$[yyyyMMddHHmmss]` can be decomposed and combined arbitrarily, such as: `$[yyyyMMdd]`, `$[HHmmss]`, `$[yyyy-MM-dd]`, etc.

- Or define by the following two ways:

    1. Use add_month(yyyyMMdd, offset) function to add or minus number of months.
      The first parameter of this function is [yyyyMMdd], represents the time format and the second parameter is offset, represents the number of months the user wants to add or minus.
        - Next N years：`$[add_months(yyyyMMdd,12*N)]`
        - N years before：`$[add_months(yyyyMMdd,-12*N)]`
        - Next N months：`$[add_months(yyyyMMdd,N)]`
        - N months before：`$[add_months(yyyyMMdd,-N)]`
      
    2. Add or minus numbers directly after the time format.
       - Next N weeks：`$[yyyyMMdd+7*N]`
       - First N weeks：`$[yyyyMMdd-7*N]`
       - Next N days：`$[yyyyMMdd+N]`
       - N days before：`$[yyyyMMdd-N]`
       - Next N hours：`$[HHmmss+N/24]`
       - First N hours：`$[HHmmss-N/24]`
       - Next N minutes：`$[HHmmss+N/24/60]`
       - First N minutes：`$[HHmmss-N/24/60]`