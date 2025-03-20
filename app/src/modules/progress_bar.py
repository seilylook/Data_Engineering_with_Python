from typing import Optional


def print_progress_bar(
    iteration: int,
    total: int,
    prefix: str = "",
    suffix: str = "",
    decimals: int = 1,
    length: int = 100,
    fill: str = "█",
    print_end: str = "\r",
) -> None:
    """
    Create a terminal progress bar that updates in-place.

    Args:
        iteration: Current iteration (0-based index)
        total: Total number of iterations
        prefix: String to display before the progress bar
        suffix: String to display after the progress bar
        decimals: Number of decimal places in the percentage
        length: Width of the progress bar in characters
        fill: Character to use for the filled portion of the bar
        print_end: String to append after the line (e.g. "\r", "\n")

    Example:
        >>> for i in range(100):
        >>>     print_progress_bar(i, 100, prefix='Progress:', suffix='Complete')
    """
    if total == 0:
        return  # Avoid division by zero

    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + "-" * (length - filled_length)

    # 마지막 반복에서만 줄바꿈 추가, 아니면 캐리지 리턴만 사용
    end_char = "\n" if iteration == total else ""

    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=end_char)
