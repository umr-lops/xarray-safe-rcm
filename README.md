# xarray-safe-rcm

Read RCM SAFE files into `xarray.DataTree` objects.

## Usage

```python
import safe_rcm

tree = safe_rcm.open_rcm(url, chunks={})
```
