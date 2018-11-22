package net.deepstorage.compscan.generator.gui;

import javax.swing.*;
import java.awt.*;

public class OkLabel extends JLabel{
   private static final Icon okIcon=new ImageIcon(UiUtil.getResource(
      "net/deepstorage/compscan/generator/gui/ok.png"
   ));
   private static final Icon editIcon=new ImageIcon(UiUtil.getResource(
      "net/deepstorage/compscan/generator/gui/editgray.png"
   ));
   
   public OkLabel(){
      this(false);
   }
   public OkLabel(boolean state){
      setValue(state);
   }
   
   public void setValue(boolean b){
      setIcon(b? okIcon: editIcon);
   }
   
   public static void main(String[] args) {
      JFrame frame=new JFrame("test");
      OkLabel ok=new OkLabel(true);
      OkLabel notOk=new OkLabel();
      frame.getContentPane().setLayout(new FlowLayout());
      frame.getContentPane().add(ok);
      frame.getContentPane().add(notOk);
      frame.pack();
      frame.show();
   }   
}
