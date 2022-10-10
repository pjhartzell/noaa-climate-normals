from typing import Optional

from stactools.core.io import ReadHrefModifier


def modify_href(
    href: str, read_href_modifier: Optional[ReadHrefModifier] = None
) -> str:
    """Modify an HREF with, for example, a token signature.

    Args:
        href (str): The HREF to be modified
        read_href_modifier (Optional[ReadHrefModifier]): An optional function
            to modify an href (e.g., to add a token to a url).

    Returns:
        str: The modified HREF.
    """
    if read_href_modifier:
        return read_href_modifier(href)
    else:
        return href
