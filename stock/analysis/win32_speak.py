import win32com.client


def say(text: str) -> None:
    speaker = win32com.client.Dispatch("SAPI.SpVoice")
    speaker.Speak(text)
    return
