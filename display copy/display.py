from kafka import KafkaConsumer
import threading
import tkinter as tk
from PIL import Image, ImageTk
import io

# Kafka Consumers
consumer_audio = KafkaConsumer(
    'Display_Audio',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='display-audio-group'
)

consumer_video = KafkaConsumer(
    'Display_Video',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='display-video-group'
)

# Helper: check if scrollbar is at bottom
def is_at_bottom(canvas):
    first, last = canvas.yview()
    return last == 1.0


# Consume Audio (append messages with mouse scroll)
def consume_audio(consumer, container, canvas):
    for message in consumer:
        data = message.value.decode('utf-8')
        lbl = tk.Label(container, text=data, font=("Arial", 12),
                       anchor="w", justify="left", bg="black", fg="white")
        lbl.pack(fill="x", padx=5, pady=2)

        # Auto scroll only if at bottom
        if is_at_bottom(canvas):
            container.update_idletasks()
            canvas.yview_moveto(1.0)


# Consume Video (mixed text + images with mouse scroll)
def consume_video(consumer, container, canvas):
    for message in consumer:
        msg_type = None
        for h in message.headers:
            if h[0] == "type":
                msg_type = h[1].decode("utf-8")

        if msg_type == "text":
            data = message.value.decode("utf-8")
            lbl = tk.Label(container, text=data, font=("Arial", 12),
                           anchor="w", justify="left", bg="black", fg="white")
            lbl.pack(fill="x", padx=5, pady=2)

        elif msg_type == "image":
            image_data = io.BytesIO(message.value)
            pil_img = Image.open(image_data).resize((300, 300))
            tk_img = ImageTk.PhotoImage(pil_img)

            img_label = tk.Label(container, image=tk_img, bg="black")
            img_label.image = tk_img  # keep reference
            img_label.pack(padx=5, pady=5)

        # Auto scroll only if at bottom
        if is_at_bottom(canvas):
            container.update_idletasks()
            canvas.yview_moveto(1.0)

def bind_mousewheel(canvas):
    def _on_mousewheel(event):
        system = canvas.tk.call("tk", "windowingsystem")

        if system == "aqua":  # macOS
            # Mouse with wheel
            if event.type == "38":  # <MouseWheel>
                canvas.yview_scroll(-1 * event.delta, "units")
            # Trackpad
            elif event.num == 4:
                canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                canvas.yview_scroll(1, "units")
        else:
            # For other OS (kept generic)
            if event.num == 4:
                canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                canvas.yview_scroll(1, "units")
            else:
                canvas.yview_scroll(-1 * int(event.delta / 120), "units")

    # Bind when mouse enters the canvas
    canvas.bind("<Enter>", lambda _: (
        canvas.bind_all("<MouseWheel>", _on_mousewheel),
        canvas.bind_all("<Button-4>", _on_mousewheel),
        canvas.bind_all("<Button-5>", _on_mousewheel)
    ))
    # Unbind when mouse leaves
    canvas.bind("<Leave>", lambda _: (
        canvas.unbind_all("<MouseWheel>"),
        canvas.unbind_all("<Button-4>"),
        canvas.unbind_all("<Button-5>")
    ))


# Utility to create a scrollable frame with mousewheel
def create_scrollable_frame(parent, width=400, height=500, bg="black"):
    canvas = tk.Canvas(parent, width=width, height=height, bg=bg, highlightthickness=0)
    scroll_frame = tk.Frame(canvas, bg=bg)

    scroll_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
    )

    canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
    canvas.pack(side=tk.LEFT, fill="both", expand=True)

    # Enable mouse + trackpad scrolling for this canvas
    bind_mousewheel(canvas)

    return scroll_frame, canvas


# GUI Setup
def start_gui():
    root = tk.Tk()
    root.title("Kafka Dual Display")
    root.geometry("1200x700")  # Larger default window size

    # Left (Audio)
    frame_left = tk.Frame(root)
    frame_left.pack(side=tk.LEFT, padx=20, pady=20, fill="both", expand=True)

    tk.Label(frame_left, text="Audio Stream", font=("Arial", 14, "bold")).pack()
    audio_frame, audio_canvas = create_scrollable_frame(frame_left, width=500, height=600, bg="black")

    # Right (Video)
    frame_right = tk.Frame(root)
    frame_right.pack(side=tk.RIGHT, padx=20, pady=20, fill="both", expand=True)

    tk.Label(frame_right, text="Video Stream", font=("Arial", 14, "bold")).pack()
    video_frame, video_canvas = create_scrollable_frame(frame_right, width=600, height=600, bg="black")

    # Threads
    threading.Thread(target=consume_audio, args=(consumer_audio, audio_frame, audio_canvas), daemon=True).start()
    threading.Thread(target=consume_video, args=(consumer_video, video_frame, video_canvas), daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    start_gui()
