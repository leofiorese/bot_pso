import tkinter as tk
from tkinter import messagebox, scrolledtext
from app.main import run_once 
import threading
import os

def run_process_in_thread(custom_date_response, days_value):
    try:
        thread = threading.Thread(target=run_once, args=(custom_date_response, days_value))
        thread.start()
        messagebox.showinfo("Iniciado", "O processo foi iniciado em segundo plano.")
    
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao iniciar o processo: {e}")

def ask_for_custom_date(root):
    
    custom_date_window = tk.Toplevel(root)
    custom_date_window.title("Configuração de Data")
    custom_date_window.geometry("400x250")

    tk.Label(custom_date_window, text="Deseja usar uma data personalizada?").pack(pady=(10, 5))
    date_var = tk.StringVar(value="não")
    
    frame_radio = tk.Frame(custom_date_window)
    tk.Radiobutton(frame_radio, text="Sim", variable=date_var, value="sim").pack(side=tk.LEFT, padx=10)
    tk.Radiobutton(frame_radio, text="Não", variable=date_var, value="não").pack(side=tk.LEFT, padx=10)
    frame_radio.pack()

    tk.Label(custom_date_window, text="Se 'Sim', informe o número de dias:").pack(pady=(10, 5))
    days_entry = tk.Entry(custom_date_window)
    days_entry.pack(pady=5)

    def on_submit():
        custom_date = date_var.get()
        days_value = None

        if custom_date == "sim":
            try:
                days_value = int(days_entry.get())
                
                if days_value <= 0:
                    messagebox.showerror("Erro", "O número de dias deve ser um inteiro positivo.", parent=custom_date_window)
                    
                    return
            
            except ValueError:
                messagebox.showerror("Erro", "Para 'Sim', você deve digitar um número válido de dias.", parent=custom_date_window)
                
                return
    
        run_process_in_thread(custom_date, days_value)
        
        custom_date_window.destroy()

    submit_button = tk.Button(custom_date_window, text="Confirmar e Iniciar", command=on_submit)
    submit_button.pack(pady=20)
    
    custom_date_window.transient(root)
    custom_date_window.grab_set()
    root.wait_window(custom_date_window)

def create_main_window():
    root = tk.Tk()
    root.title("PSOffice Bot Interface")
    root.geometry("650x450")
    
    tk.Label(root, text="PSOffice Bot - Busca de Relatórios Personalizados", font=("Arial", 16)).pack(pady=20)
    
    run_button = tk.Button(root, text="Iniciar Pequisa", width=44, height=2, command=lambda: ask_for_custom_date(root))
    run_button.pack(pady=10)

    action_frame = tk.Frame(root)
    action_frame.pack(pady=(5, 10))

    close_button = tk.Button(action_frame, text="Fechar", width=20, height=2, command=root.quit)
    close_button.pack(side=tk.LEFT, padx=10)

    log_label = tk.Label(root, text="Logs do Sistema:", font=("Arial", 10))
    log_label.pack(pady=(10, 0), padx=10, anchor="w")

    log_frame = tk.Frame(root)
    log_frame.pack(pady=10, padx=5, fill=tk.BOTH, expand=True)

    log_viewer = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state='disabled', font=("Courier New", 9))
    log_viewer.pack(fill=tk.BOTH, expand=True)

    def update_log_viewer():
        log_file_path = './pso_bot.log'

        try:
            with open(log_file_path, 'r', encoding='latin-1') as log_file:
                log_content = log_file.read()

            log_viewer.config(state='normal')
            log_viewer.delete(1.0, tk.END)
            log_viewer.insert(tk.END, log_content)
            log_viewer.see(tk.END)
            log_viewer.config(state='disabled')
        
        except FileNotFoundError:
            log_viewer.config(state='normal')
            log_viewer.delete(1.0, tk.END)
            log_viewer.insert(tk.END, "Arquivo de log não encontrado.")
            log_viewer.config(state='disabled')

        root.after(5000, update_log_viewer)

    update_log_viewer()

    root.mainloop()

if __name__ == '__main__':
    create_main_window()