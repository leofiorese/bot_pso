import tkinter as tk
from tkinter import messagebox, scrolledtext
from main import run_once, get_base_path 
import threading
import os

def run_process_in_thread(custom_date_response, days_value, script_choice):
    try:
        thread = threading.Thread(target=run_once, args=(custom_date_response, days_value, script_choice))
        thread.start()

        auto_close = tk.Toplevel()
        auto_close.title("Iniciado")
        auto_close.geometry("350x120")
        auto_close.resizable(False, False)
        auto_close.attributes("-topmost", True)

        tk.Label(auto_close, text="O processo foi iniciado em segundo plano.", font=("Arial", 10), wraplength=300, justify="center").pack(pady=20)

        def close_popup():
            if auto_close.winfo_exists():
                auto_close.destroy()

        auto_close.after(5000, close_popup)

        tk.Button(auto_close, text="OK", width=10, command=close_popup).pack(pady=5)

        auto_close.transient()
        auto_close.grab_set()
        auto_close.focus_force()
        
        auto_close.after(10, lambda: auto_close.focus())

    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao iniciar o processo: {e}") 

def ask_for_script_choice(root, custom_date_response, days_value):
    script_choice_window = tk.Toplevel(root)
    script_choice_window.title("Escolha do Script")
    script_choice_window.geometry("400x250")

    tk.Label(script_choice_window, text="Escolha qual script utilizar para a pesquisa:").pack(pady=(10, 5))

    script_choice = tk.StringVar(value="Orçado")

    frame_radio = tk.Frame(script_choice_window)
    tk.Radiobutton(frame_radio, text="Orçado", variable=script_choice, value="Orçado").pack(side=tk.LEFT, padx=10)
    tk.Radiobutton(frame_radio, text="Planejado", variable=script_choice, value="Planejado").pack(side=tk.LEFT, padx=10)
    tk.Radiobutton(frame_radio, text="Realizado", variable=script_choice, value="Realizado").pack(side=tk.LEFT, padx=10)
    frame_radio.pack()

    submitted = False
    TIMEOUT_MS = 10000  # 10 segundos

    def on_timeout():
        nonlocal submitted
        if submitted:
            return
        submitted = True
        script_choice_window.destroy()
        
        ask_for_custom_date(root, custom_date_response, days_value, "Realizado")

    timeout_id = script_choice_window.after(TIMEOUT_MS, on_timeout)

    def on_submit():
        nonlocal submitted
        if submitted:
            return
        submitted = True
        try:
            script_choice_window.after_cancel(timeout_id)
        except:
            pass
        script_choice_selected = script_choice.get()
        script_choice_window.destroy()
        ask_for_custom_date(root, custom_date_response, days_value, script_choice_selected)

    submit_button = tk.Button(script_choice_window, text="Confirmar e Iniciar", command=on_submit)
    submit_button.pack(pady=20)

    script_choice_window.transient(root)
    script_choice_window.grab_set()
    root.wait_window(script_choice_window)

def ask_for_custom_date(root, custom_date_response, days_value, script_choice):
    
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

    submitted = False
    TIMEOUT_MS = 10000  # 10 segundos

    def on_timeout():
        nonlocal submitted
        
        if submitted:
            return
        
        submitted = True
        
        run_process_in_thread("não", None, script_choice)
        
        try:
            custom_date_window.destroy()
        
        except:
            pass

    timeout_id = custom_date_window.after(TIMEOUT_MS, on_timeout)

    def on_submit():
        nonlocal submitted
        if submitted:
            return
        submitted = True

        try:
            custom_date_window.after_cancel(timeout_id)
        except:
            pass

        custom_date = date_var.get()
        days_value = None

        if custom_date == "sim":
            try:
                days_value = int(days_entry.get())
                
                if days_value <= 0:
                    messagebox.showerror("Erro", "O número de dias deve ser um inteiro positivo.", parent=custom_date_window)
                    submitted = False 
                    return
            except ValueError:
                messagebox.showerror("Erro", "Para 'Sim', você deve digitar um número válido de dias.", parent=custom_date_window)
                submitted = False
                return
    
        run_process_in_thread(custom_date, days_value, script_choice)
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
    
    run_button = tk.Button(root, text="Iniciar Pequisa", width=44, height=2, command=lambda: ask_for_script_choice(root, "não", None))
    run_button.pack(pady=10)

    def auto_click_button():
        run_button.invoke()

    # 10 segundos para clicar automaticamente no botão "Iniciar Pesquisa" caso o usuário não clique
    root.after(10000, auto_click_button)

    action_frame = tk.Frame(root)
    action_frame.pack(pady=(5, 10))

    def clear_log_file():
        BASE_PATH = get_base_path()
        log_file_path = os.path.join(BASE_PATH, "pso_bot.log")
        
        try:
            with open(log_file_path, 'w', encoding='latin-1') as f:
                f.write(f"")
        
        except Exception as e:
            log_viewer.config(state='normal')
            log_viewer.insert(tk.END, f"\nERRO ao limpar o arquivo de log: {e}\n")
            log_viewer.config(state='disabled')

    clear_log = tk.Button(action_frame, text="Limpar Log", width=20, height=2, command=clear_log_file)
    clear_log.pack(side=tk.LEFT, padx=10)

    def clear_log_and_close():
        clear_log_file()
        root.destroy()

    close_button = tk.Button(action_frame, text="Fechar", width=20, height=2, command=clear_log_and_close)
    close_button.pack(side=tk.LEFT, padx=10)

    log_label = tk.Label(root, text="Logs do Sistema:", font=("Arial", 10))
    log_label.pack(pady=(10, 0), padx=10, anchor="w")

    log_frame = tk.Frame(root)
    log_frame.pack(pady=10, padx=5, fill=tk.BOTH, expand=True)

    log_viewer = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state='disabled', font=("Courier New", 9))
    log_viewer.pack(fill=tk.BOTH, expand=True)

    def update_log_viewer():
        BASE_PATH = get_base_path()
        log_file_path = os.path.join(BASE_PATH, "pso_bot.log")

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
