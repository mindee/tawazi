from typing import List, Optional, Tuple, Union

IdentityHash = str
Tag = Union[None, str, tuple]  # anything immutable

ARG_NAME_TAG = "twz_tag"

RESERVED_KWARGS = [ARG_NAME_TAG]
ARG_NAME_SEP = ">>>"
USE_SEP_START = "<<"
USE_SEP_END = ">>"
ReturnIDsType = Optional[Union[List[IdentityHash], Tuple[IdentityHash], IdentityHash]]
