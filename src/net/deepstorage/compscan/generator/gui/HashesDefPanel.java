package net.deepstorage.compscan.generator.gui;

import java.awt.*;
import java.io.*;
import javax.swing.*;
import javax.swing.text.*;
import java.util.function.*;

import net.deepstorage.compscan.generator.*;

public class HashesDefPanel extends JPanel{
   private final String byFileId="BY_FILE";
   private final String byRatioId="BY_RATIO";
   private final JRadioButton byFileBtn = new JRadioButton("From hashes file");
   private final JRadioButton byRatioBtn = new JRadioButton("By deduplication ratio");
   {
      ButtonGroup g = new ButtonGroup();
      g.add(byFileBtn);
      g.add(byRatioBtn);
   }
   
   private final JTextComponent filePathField=new JEditorPane();
   private final OkLabel fileOkLabel=new OkLabel();
   private final JButton browseBtn=new JButton("Browse");
   
   private final NumberField sizeField=new NumberField.Natural(10);
   private final JComboBox sizeUnits=new JComboBox(new String[]{
      "B",
      "KiB",
      "MiB",
      "GiB",
      "blocks"
   });
   private final int[] sizeUnitValues={
      1, 1024, 1024*1024, 1024*1024*1024
   };
   private final OkLabel sizeOkLabel=new OkLabel();
   private final int I_B=0;
   private final int I_KIB=1;
   private final int I_MIB=2;
   private final int I_GIB=3;
   private final int I_BLOCKS=5;
   
   private final NumberField ratioField=new NumberField.Ratio(10);
   private final OkLabel ratioOkLabel=new OkLabel();
   
   private final NumberField blockSizeField;
   private final App app;
   
   public HashesDefPanel(App app, NumberField blockSizeField){
      this.app=app;
      this.blockSizeField=blockSizeField;
      filePathField.setBorder(ratioField.getBorder());
      setLayout(new GridLayout(1,1));
      Component byFileCard=UiUtil.makeVBox(1,0,1, 2, new Component[]{
         UiUtil.vglue(),
         filePathField,
         UiUtil.makeHBox(1,0,0, 5, new Component[]{
            UiUtil.hglue(), fileOkLabel, UiUtil.hgap(5), browseBtn
         }),
         UiUtil.vglue()
      });
      Component byRatioCard=UiUtil.makeVBox(0,0,1, 2, new Component[]{
         UiUtil.makeHBox(0,0,1, 5, new Component[]{
            UiUtil.hgap(10),
            new JLabel("Total size"), 
            UiUtil.hgap(10),
            sizeField, UiUtil.hgap(1), 
            sizeUnits, UiUtil.hgap(5), 
            sizeOkLabel,
            
            UiUtil.hgap(50),
            
            new JLabel("Ratio"),
            UiUtil.hgap(10),
            ratioField, UiUtil.hgap(5), 
            ratioOkLabel,
            
            UiUtil.hglue()
         }),
         UiUtil.vglue()
      });
      Container cards=new JPanel();
      CardLayout cardLayout=new CardLayout();
      cards.setLayout(cardLayout);
      cards.add(byRatioCard);
      cards.add(byFileCard);
      cardLayout.addLayoutComponent(byFileCard, byFileId);
      cardLayout.addLayoutComponent(byRatioCard, byRatioId);
      byFileBtn.addActionListener(e->{
         cardLayout.show(cards, byFileId);
      });
      byRatioBtn.addActionListener(e->{
         cardLayout.show(cards, byRatioId);
      });
      add(UiUtil.makeVBox(0,0,1, 5, new Component[]{
         UiUtil.makeHBox(0,0,1, 5, new Component[]{
            byFileBtn, byRatioBtn, UiUtil.hglue()
         }),
         cards,
         UiUtil.vglue()
      }));
      filePathField.getDocument().addDocumentListener(
         UiUtil.onAnyDocumentChange(()->{
            File f=new File(filePathField.getText());
            fileOkLabel.setValue(f.exists() && f.isFile());
         })
      );
      browseBtn.addActionListener(e->{
         filePathField.setText(UiUtil.requestFile(
            HashesDefPanel.this, AppConfig.CSV_FILES, null
         ).getPath());
      });
      sizeField.listeners.add(v->checkSize());
      ratioField.listeners.add(v->ratioOkLabel.setValue(v!=null));
      sizeUnits.addActionListener(e->checkSize());
      byFileBtn.getModel().setSelected(true);
      cardLayout.show(cards, byFileId);
      checkSize();
   }
   
   public Supplier<int[][]> getValue(){
      if(byFileBtn.getModel().isSelected()){
         return new HashesDefFile(filePathField.getText());
      }
      
      if(sizeField.currentValue==null) return null;
      if(ratioField.currentValue==null) return null;
      
      int i=sizeUnits.getSelectedIndex();
      if(
         i>=I_BLOCKS && (
            blockSizeField ==null || blockSizeField.currentValue==null
         )
      ){
         return null;
      }
      
      HashesDefGenerator g=new HashesDefGenerator(app);
      g.totalSize=sizeField.currentValue.intValue() * (
         i>=I_BLOCKS? blockSizeField.currentValue.intValue(): sizeUnitValues[i]
      );
      g.ratio=ratioField.currentValue.floatValue();
      return g;
   }
   
   private void checkSize(){
      boolean sizeOk= sizeField.currentValue!=null;
      if(sizeUnits.getSelectedIndex()==I_BLOCKS) sizeOk &= 
         blockSizeField!=null && blockSizeField.currentValue!=null
      ;
      sizeOkLabel.setValue(sizeOk);
   }
   
   public static void main(String[] args) {
      JFrame frame=new JFrame("test");
      frame.getContentPane().add(new HashesDefPanel(null,null));
      frame.pack();
      frame.show();
   }   
}
