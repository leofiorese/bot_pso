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
    root.geometry("600x400")
    
    tk.Label(root, text="PSOffice Bot - Busca de Relatórios Personalizados", font=("Arial", 16)).pack(pady=20)
    
    run_button = tk.Button(root, text="Iniciar Pequisa", width=20, height=2, command=lambda: ask_for_custom_date(root))
    run_button.pack(pady=10)

    root.mainloop()

if __name__ == '__main__':
    create_main_window()